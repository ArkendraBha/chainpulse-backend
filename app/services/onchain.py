import datetime
import logging
from app.core.cache import cache_get, cache_set

logger = logging.getLogger("chainpulse")

COINGLASS_API_KEY = __import__("os").getenv("COINGLASS_API_KEY", "")

BINANCE_SYMBOL_MAP = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "BNB": "BNBUSDT",
    "AVAX": "AVAXUSDT",
    "LINK": "LINKUSDT",
    "ADA": "ADAUSDT",
}

COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "BNB": "binancecoin",
    "AVAX": "avalanche-2",
    "LINK": "chainlink",
    "ADA": "cardano",
}


async def get_funding_rates(coin: str) -> dict:
    """
    Fetches perpetual funding rates from Binance.
    High positive funding = overleveraged longs = bearish contrarian signal.
    High negative funding = overleveraged shorts = bullish contrarian signal.
    """
    cache_key = f"funding:{coin}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    symbol = BINANCE_SYMBOL_MAP.get(coin.upper())
    if not symbol:
        return {"available": False, "reason": f"Unsupported coin: {coin}"}

    try:
        import httpx as _httpx

        async with _httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "[fapi.binance.com](https://fapi.binance.com/fapi/v1/fundingRate)",
                params={"symbol": symbol, "limit": 8},
            )
            r.raise_for_status()
            rates = r.json()

        if not rates:
            return {"available": False, "reason": "No data returned"}

        current_rate = float(rates[-1]["fundingRate"])
        avg_rate = sum(float(r["fundingRate"]) for r in rates) / len(rates)
        annualized = current_rate * 3 * 365 * 100

        if current_rate > 0.001:
            signal = "overleveraged_longs"
            sentiment = "bearish_contrarian"
            interpretation = (
                f"Longs are paying {round(current_rate * 100, 3)}% per 8h. "
                f"Overcrowded long positioning — bearish contrarian signal."
            )
        elif current_rate < -0.001:
            signal = "overleveraged_shorts"
            sentiment = "bullish_contrarian"
            interpretation = (
                f"Shorts are paying {round(abs(current_rate) * 100, 3)}% per 8h. "
                f"Overcrowded short positioning — bullish contrarian signal."
            )
        else:
            signal = "balanced"
            sentiment = "neutral"
            interpretation = (
                f"Funding balanced at {round(current_rate * 100, 4)}%. "
                f"No extreme positioning detected."
            )

        result = {
            "available": True,
            "coin": coin,
            "current_rate_pct": round(current_rate * 100, 4),
            "avg_rate_8period_pct": round(avg_rate * 100, 4),
            "annualized_pct": round(annualized, 1),
            "signal": signal,
            "sentiment": sentiment,
            "interpretation": interpretation,
            "data_source": "binance_perpetuals",
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }

        cache_set(cache_key, result, ttl=300)
        return result

    except Exception as e:
        logger.error(f"Funding rate fetch failed for {coin}: {e}")
        return {"available": False, "error": str(e)}


async def get_open_interest(coin: str) -> dict:
    """
    Tracks open interest changes as a regime confirmation signal.
    Rising OI + rising price = strong trend confirmation.
    Rising OI + falling price = strong downtrend confirmation.
    Falling OI = trend exhaustion signal.
    """
    cache_key = f"oi:{coin}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    symbol = BINANCE_SYMBOL_MAP.get(coin.upper())
    if not symbol:
        return {"available": False, "reason": f"Unsupported coin: {coin}"}

    try:
        import httpx as _httpx

        async with _httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "[fapi.binance.com](https://fapi.binance.com/futures/data/openInterestHist)",
                params={"symbol": symbol, "period": "1h", "limit": 24},
            )
            r.raise_for_status()
            data = r.json()

        if not data:
            return {"available": False, "reason": "No OI data"}

        oi_values = [float(d["sumOpenInterest"]) for d in data]
        oi_usd_values = [float(d["sumOpenInterestValue"]) for d in data]

        current_oi = oi_values[-1]
        oi_24h_ago = oi_values[0]
        current_oi_usd = oi_usd_values[-1]

        oi_change_pct = (
            ((current_oi - oi_24h_ago) / oi_24h_ago) * 100 if oi_24h_ago > 0 else 0
        )

        if oi_change_pct > 5:
            oi_signal = "increasing"
            interpretation = (
                f"OI up {round(oi_change_pct, 1)}% in 24h. "
                f"New money entering — trend continuation likely."
            )
        elif oi_change_pct < -5:
            oi_signal = "decreasing"
            interpretation = (
                f"OI down {round(abs(oi_change_pct), 1)}% in 24h. "
                f"Position unwinding — possible trend exhaustion."
            )
        else:
            oi_signal = "stable"
            interpretation = (
                f"OI stable ({round(oi_change_pct, 1)}% change). "
                f"No significant positioning shift."
            )

        result = {
            "available": True,
            "coin": coin,
            "current_oi_contracts": round(current_oi, 0),
            "current_oi_usd_millions": round(current_oi_usd / 1_000_000, 1),
            "oi_change_24h_pct": round(oi_change_pct, 2),
            "oi_signal": oi_signal,
            "interpretation": interpretation,
            "data_source": "binance_futures",
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }

        cache_set(cache_key, result, ttl=300)
        return result

    except Exception as e:
        logger.error(f"Open interest fetch failed for {coin}: {e}")
        return {"available": False, "error": str(e)}


async def get_combined_onchain(coin: str) -> dict:
    """
    Fetches all on-chain metrics for a coin in one call.
    Returns funding rates + open interest + composite signal.
    """
    import asyncio

    funding, oi = await asyncio.gather(
        get_funding_rates(coin),
        get_open_interest(coin),
        return_exceptions=True,
    )

    if isinstance(funding, Exception):
        funding = {"available": False, "error": str(funding)}
    if isinstance(oi, Exception):
        oi = {"available": False, "error": str(oi)}

    # Composite signal
    composite_signal = "neutral"
    composite_notes = []

    if funding.get("available"):
        if funding["signal"] == "overleveraged_longs":
            composite_notes.append("Funding bearish (longs crowded)")
        elif funding["signal"] == "overleveraged_shorts":
            composite_notes.append("Funding bullish (shorts crowded)")

    if oi.get("available"):
        if oi["oi_signal"] == "increasing":
            composite_notes.append("OI increasing (trend confirmation)")
        elif oi["oi_signal"] == "decreasing":
            composite_notes.append("OI decreasing (trend exhaustion risk)")

    if composite_notes:
        composite_signal = " | ".join(composite_notes)

    return {
        "coin": coin,
        "funding_rates": funding,
        "open_interest": oi,
        "composite_signal": composite_signal,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }
