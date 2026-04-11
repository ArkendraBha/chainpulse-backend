from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.security import get_auth_header
from app.core.rate_limit import rate_limiter
from app.core.cache import cache_get, cache_set
from app.db.database import get_db
from app.auth.auth import require_tier, update_last_active
from app.services.onchain import (
    get_funding_rates,
    get_open_interest,
    get_combined_onchain,
)

router = APIRouter()


@router.get("/funding-rates/{coin}")
async def funding_rates_endpoint(
    request: Request,
    coin: str,
    db: Session = Depends(get_db),
):
    """
    Get perpetual funding rates for a coin.
    Extreme funding is a contrarian signal.
    Available: Essential tier and above.
    """
    rate_limiter.require(request, max_requests=20, window_seconds=60)
    coin = coin.upper()
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")

    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")
    update_last_active(request, db)

    return await get_funding_rates(coin)


@router.get("/open-interest/{coin}")
async def open_interest_endpoint(
    request: Request,
    coin: str,
    db: Session = Depends(get_db),
):
    """
    Get open interest data for a coin.
    Rising OI confirms trend. Falling OI signals exhaustion.
    Available: Essential tier and above.
    """
    rate_limiter.require(request, max_requests=20, window_seconds=60)
    coin = coin.upper()
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")

    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")
    update_last_active(request, db)

    return await get_open_interest(coin)


@router.get("/onchain/{coin}")
async def onchain_combined_endpoint(
    request: Request,
    coin: str,
    db: Session = Depends(get_db),
):
    """
    Get all on-chain metrics for a coin in one call.
    Includes funding rates, open interest, and composite signal.
    Available: Pro tier and above.
    """
    rate_limiter.require(request, max_requests=10, window_seconds=60)
    coin = coin.upper()
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")

    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)

    cache_key = f"onchain_combined:{coin}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    result = await get_combined_onchain(coin)
    cache_set(cache_key, result, ttl=300)
    return result


@router.get("/onchain-overview")
async def onchain_overview_endpoint(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Get on-chain metrics overview for all supported coins.
    Available: Pro tier and above.
    """
    rate_limiter.require(request, max_requests=5, window_seconds=60)
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)

    cache_key = "onchain_overview"
    cached = cache_get(cache_key)
    if cached:
        return cached

    import asyncio
    results = await asyncio.gather(
        *[get_combined_onchain(coin) for coin in settings.SUPPORTED_COINS],
        return_exceptions=True,
    )

    overview = []
    for coin, result in zip(settings.SUPPORTED_COINS, results):
        if isinstance(result, Exception):
            overview.append({"coin": coin, "available": False})
        else:
            overview.append(result)

    response = {
        "coins": overview,
        "timestamp": __import__("datetime").datetime.utcnow().isoformat(),
    }
    cache_set(cache_key, response, ttl=300)
    return response
