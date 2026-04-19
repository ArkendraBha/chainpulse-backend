import datetime
import logging
from sqlalchemy.orm import Session
from app.db.models import MarketSummary

logger = logging.getLogger("chainpulse")

EXPOSURE_MAP = {
    "Strong Risk-On": 0.85,
    "Risk-On": 0.65,
    "Neutral": 0.40,
    "Risk-Off": 0.20,
    "Strong Risk-Off": 0.05,
}

STRATEGY_DESCRIPTIONS = {
    "follow_model": "Follow ChainPulse recommended exposure for each regime",
    "buy_and_hold": "Always 100% exposed regardless of regime",
    "risk_off_only": "10% in Risk-Off regimes, 80% otherwise",
    "momentum": "85% in Risk-On regimes, 5% otherwise",
    "inverse": "High exposure in Risk-Off, low in Risk-On (contrarian)",
}


def run_backtest(
    db: Session,
    coin: str,
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    initial_capital: float = 10000,
    strategy: str = "follow_model",
    rebalance_frequency_hours: int = 4,
) -> dict:
    """
    Backtests a strategy against historical regime data.

    Uses regime score momentum as a price proxy.
    Results are directional estimates — not precise P&L.

    Strategies:
      follow_model  - Use ChainPulse recommended exposure
      buy_and_hold  - Always 100% exposed
      risk_off_only - Reduce to 10% in Risk-Off
      momentum      - Only hold in Risk-On regimes
      inverse       - Contrarian (high in Risk-Off)
    """
    records_1h = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "1h",
            MarketSummary.created_at >= start_date,
            MarketSummary.created_at <= end_date,
        )
        .order_by(MarketSummary.created_at.asc())
        .all()
    )

    if len(records_1h) < 24:
        return {
            "error": "Insufficient historical data",
            "available_records": len(records_1h),
            "required": 24,
            "message": (
                f"Only {len(records_1h)} records available. "
                f"Need at least 24 hours of data."
            ),
        }

    equity_curve = [initial_capital]
    benchmark_curve = [initial_capital]
    trades = []
    current_exposure = 0.5
    last_rebalance = records_1h[0].created_at

    regime_performance = {}

    for i in range(1, len(records_1h)):
        record = records_1h[i]
        prev_record = records_1h[i - 1]

        # Price return estimate from score momentum
        score_change = record.score - prev_record.score
        price_return_estimate = score_change * 0.002

        # Rebalance check
        hours_since = (record.created_at - last_rebalance).total_seconds() / 3600

        if hours_since >= rebalance_frequency_hours:
            if strategy == "follow_model":
                target = EXPOSURE_MAP.get(record.label, 0.4)
            elif strategy == "buy_and_hold":
                target = 1.0
            elif strategy == "risk_off_only":
                target = 0.10 if "Risk-Off" in record.label else 0.80
            elif strategy == "momentum":
                target = 0.85 if "Risk-On" in record.label else 0.05
            elif strategy == "inverse":
                target = (
                    0.80
                    if "Risk-Off" in record.label
                    else 0.05 if "Risk-On" in record.label else 0.40
                )
            else:
                target = 0.5

            if abs(target - current_exposure) > 0.05:
                trades.append(
                    {
                        "timestamp": record.created_at.isoformat(),
                        "regime": record.label,
                        "from_exposure": round(current_exposure * 100, 1),
                        "to_exposure": round(target * 100, 1),
                        "direction": (
                            "increase" if target > current_exposure else "decrease"
                        ),
                    }
                )
                current_exposure = target
                last_rebalance = record.created_at

        # Apply returns
        strategy_return = price_return_estimate * current_exposure
        benchmark_return = price_return_estimate * 1.0

        new_equity = equity_curve[-1] * (1 + strategy_return)
        new_benchmark = benchmark_curve[-1] * (1 + benchmark_return)

        equity_curve.append(round(new_equity, 2))
        benchmark_curve.append(round(new_benchmark, 2))

        # Track by regime
        label = record.label
        if label not in regime_performance:
            regime_performance[label] = {
                "hours": 0,
                "strategy_returns": [],
                "benchmark_returns": [],
            }
        regime_performance[label]["hours"] += 1
        regime_performance[label]["strategy_returns"].append(strategy_return)
        regime_performance[label]["benchmark_returns"].append(benchmark_return)

    # Statistics
    final_equity = equity_curve[-1]
    final_benchmark = benchmark_curve[-1]
    total_return = ((final_equity - initial_capital) / initial_capital) * 100
    benchmark_return_total = (
        (final_benchmark - initial_capital) / initial_capital
    ) * 100
    alpha = round(total_return - benchmark_return_total, 2)

    # Max drawdown
    peak = initial_capital
    max_dd = 0.0
    for val in equity_curve:
        if val > peak:
            peak = val
        dd = ((peak - val) / peak) * 100
        if dd > max_dd:
            max_dd = dd

    # Sharpe ratio
    returns = [
        (equity_curve[i] - equity_curve[i - 1]) / equity_curve[i - 1]
        for i in range(1, len(equity_curve))
        if equity_curve[i - 1] > 0
    ]
    if len(returns) > 1:
        avg_r = sum(returns) / len(returns)
        std_r = (sum((r - avg_r) ** 2 for r in returns) / len(returns)) ** 0.5
        sharpe = (avg_r / std_r) * (8760**0.5) if std_r > 0 else 0
    else:
        sharpe = 0

    # Win rate
    winning = sum(
        1 for i in range(1, len(equity_curve)) if equity_curve[i] > equity_curve[i - 1]
    )
    win_rate = (
        round((winning / (len(equity_curve) - 1)) * 100, 1)
        if len(equity_curve) > 1
        else 0
    )

    # Regime breakdown
    regime_summary = {}
    for label, data in regime_performance.items():
        sr = data["strategy_returns"]
        br = data["benchmark_returns"]
        if sr:
            regime_summary[label] = {
                "hours": data["hours"],
                "strategy_avg_return_pct": round(sum(sr) / len(sr) * 100, 4),
                "benchmark_avg_return_pct": (
                    round(sum(br) / len(br) * 100, 4) if br else 0
                ),
                "pct_of_period": round((data["hours"] / len(records_1h)) * 100, 1),
            }

    # Sampled equity curve for chart
    sample_step = max(1, len(equity_curve) // 200)
    sampled_curve = []
    for i in range(0, len(equity_curve), sample_step):
        if i < len(records_1h):
            sampled_curve.append(
                {
                    "timestamp": records_1h[i].created_at.isoformat(),
                    "equity": equity_curve[i],
                    "benchmark": benchmark_curve[i],
                    "regime": records_1h[i].label,
                    "exposure": round(current_exposure * 100, 0),
                }
            )

    return {
        "coin": coin,
        "strategy": strategy,
        "strategy_description": STRATEGY_DESCRIPTIONS.get(strategy, strategy),
        "period": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "days": (end_date - start_date).days,
            "data_points": len(records_1h),
        },
        "results": {
            "initial_capital": initial_capital,
            "final_capital": round(final_equity, 2),
            "total_return_pct": round(total_return, 2),
            "benchmark_return_pct": round(benchmark_return_total, 2),
            "alpha_pct": alpha,
            "max_drawdown_pct": round(max_dd, 2),
            "sharpe_ratio": round(sharpe, 3),
            "win_rate_pct": win_rate,
            "total_trades": len(trades),
        },
        "regime_breakdown": regime_summary,
        "equity_curve": sampled_curve,
        "recent_trades": trades[-10:],
        "disclaimer": (
            "Uses regime score momentum as a price proxy. "
            "Directional estimates only — not precise P&L. "
            "Not financial advice."
        ),
    }


def compare_strategies(
    db: Session,
    coin: str,
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    initial_capital: float = 10000,
) -> dict:
    """
    Runs all strategies and returns a comparison table.
    Useful for showing which strategy performed best in each regime period.
    """
    results = {}
    for strategy in STRATEGY_DESCRIPTIONS.keys():
        result = run_backtest(
            db,
            coin,
            start_date,
            end_date,
            initial_capital,
            strategy,
        )
        if "error" not in result:
            results[strategy] = {
                "total_return_pct": result["results"]["total_return_pct"],
                "alpha_pct": result["results"]["alpha_pct"],
                "max_drawdown_pct": result["results"]["max_drawdown_pct"],
                "sharpe_ratio": result["results"]["sharpe_ratio"],
                "total_trades": result["results"]["total_trades"],
                "description": result["strategy_description"],
            }

    if not results:
        return {"error": "Insufficient data for comparison"}

    best_strategy = max(
        results.items(),
        key=lambda x: x[1]["total_return_pct"],
    )[0]
    best_sharpe = max(
        results.items(),
        key=lambda x: x[1]["sharpe_ratio"],
    )[0]

    return {
        "coin": coin,
        "period_days": (end_date - start_date).days,
        "strategies": results,
        "best_return": best_strategy,
        "best_risk_adjusted": best_sharpe,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }
