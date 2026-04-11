import random
import math
import datetime
import logging

logger = logging.getLogger("chainpulse")


def monte_carlo_var(
    exposure_pct: float,
    account_size: float,
    hazard_rate: float,
    volatility_score: float,
    simulations: int = 10000,
    horizon_days: int = 7,
) -> dict:
    """
    Monte Carlo Value at Risk simulation.

    Uses regime-conditioned return distribution:
    - Daily volatility scaled by volatility_score
    - Hazard rate increases tail risk probability
    - Regime collapse events added as tail shocks

    Returns VaR at 95% and 99% confidence levels.
    """
    # Base daily volatility from volatility score
    daily_vol = (volatility_score / 100) * 0.04
    hazard_adj = 1 + (hazard_rate / 100) * 0.5
    adj_vol = daily_vol * hazard_adj

    # Slight negative drift when hazard is elevated
    daily_drift = -0.0002 * (hazard_rate / 100)

    results = []
    portfolio_value_at_risk = account_size * (exposure_pct / 100)

    for _ in range(simulations):
        value = portfolio_value_at_risk
        for _ in range(horizon_days):
            # Normal daily shock
            shock = random.gauss(daily_drift, adj_vol)

            # Regime collapse tail event
            if random.random() < (hazard_rate / 100) * 0.02:
                shock -= random.uniform(0.05, 0.25)

            # Volatility spike event
            if random.random() < 0.02:
                shock += random.gauss(0, adj_vol * 2)

            value *= (1 + shock)
            value = max(0, value)

        pnl_pct = (
            (value - portfolio_value_at_risk) / account_size
        ) * 100
        results.append(round(pnl_pct, 4))

    results.sort()

    idx_95 = int(simulations * 0.05)
    idx_99 = int(simulations * 0.01)
    idx_995 = int(simulations * 0.005)

    var_95 = results[idx_95]
    var_99 = results[idx_99]
    var_995 = results[idx_995]

    # Conditional VaR (Expected Shortfall)
    cvar_95 = sum(results[:idx_95]) / idx_95 if idx_95 > 0 else var_95
    cvar_99 = sum(results[:idx_99]) / idx_99 if idx_99 > 0 else var_99

    # Percentile outcomes
    p5 = results[int(simulations * 0.05)]
    p25 = results[int(simulations * 0.25)]
    p50 = results[int(simulations * 0.50)]
    p75 = results[int(simulations * 0.75)]
    p95 = results[int(simulations * 0.95)]

    probability_of_loss = (
        sum(1 for r in results if r < 0) / simulations * 100
    )
    probability_loss_5pct = (
        sum(1 for r in results if r < -5) / simulations * 100
    )
    probability_loss_10pct = (
        sum(1 for r in results if r < -10) / simulations * 100
    )
    probability_loss_20pct = (
        sum(1 for r in results if r < -20) / simulations * 100
    )

    # Dollar values
    var_95_usd = abs(round(account_size * var_95 / 100, 2))
    var_99_usd = abs(round(account_size * var_99 / 100, 2))
    cvar_95_usd = abs(round(account_size * cvar_95 / 100, 2))

    interpretation = (
        f"At {exposure_pct}% exposure over {horizon_days} days: "
        f"95% VaR is {abs(var_95):.1f}% (${var_95_usd:,.0f}). "
        f"There is a {probability_of_loss:.0f}% probability of any loss "
        f"and a {probability_loss_10pct:.1f}% chance of losing more than 10% "
        f"of your portfolio."
    )

    return {
        "simulation_params": {
            "simulations": simulations,
            "horizon_days": horizon_days,
            "exposure_pct": exposure_pct,
            "account_size": account_size,
            "hazard_rate_input": hazard_rate,
            "volatility_score_input": volatility_score,
            "adjusted_daily_vol_pct": round(adj_vol * 100, 3),
        },
        "value_at_risk": {
            "var_95_pct": round(var_95, 2),
            "var_99_pct": round(var_99, 2),
            "var_995_pct": round(var_995, 2),
            "var_95_usd": var_95_usd,
            "var_99_usd": var_99_usd,
        },
        "conditional_var": {
            "cvar_95_pct": round(cvar_95, 2),
            "cvar_99_pct": round(cvar_99, 2),
            "cvar_95_usd": cvar_95_usd,
        },
        "outcome_distribution": {
            "worst_5pct": round(p5, 2),
            "p25": round(p25, 2),
            "median_pct": round(p50, 2),
            "p75": round(p75, 2),
            "best_5pct": round(p95, 2),
        },
        "loss_probabilities": {
            "any_loss_pct": round(probability_of_loss, 1),
            "loss_gt_5pct": round(probability_loss_5pct, 2),
            "loss_gt_10pct": round(probability_loss_10pct, 2),
            "loss_gt_20pct": round(probability_loss_20pct, 2),
        },
        "dollar_at_risk": {
            "portfolio_at_risk": round(portfolio_value_at_risk, 2),
            "expected_loss_95pct": var_95_usd,
            "expected_loss_99pct": var_99_usd,
            "worst_case_usd": abs(round(account_size * results[0] / 100, 2)),
        },
        "interpretation": interpretation,
        "disclaimer": (
            "Monte Carlo simulation using regime-conditioned parameters. "
            "Not financial advice. Past regime behavior does not predict future results."
        ),
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


def kelly_criterion(
    win_rate: float,
    avg_win_pct: float,
    avg_loss_pct: float,
    account_size: float,
    regime_label: str = "Neutral",
    hazard: float = 50,
) -> dict:
    """
    Kelly Criterion optimal position sizing
    adjusted for current regime conditions.
    """
    b = avg_win_pct / avg_loss_pct
    p = win_rate
    q = 1 - win_rate

    full_kelly = (b * p - q) / b
    full_kelly = max(0, full_kelly)

    half_kelly = full_kelly / 2
    quarter_kelly = full_kelly / 4

    regime_mult = {
        "Strong Risk-On": 1.0,
        "Risk-On": 0.85,
        "Neutral": 0.60,
        "Risk-Off": 0.35,
        "Strong Risk-Off": 0.10,
    }.get(regime_label, 0.60)

    hazard_mult = 1 - (hazard / 100) * 0.5
    adjusted_kelly = max(0, full_kelly * regime_mult * hazard_mult)
    recommendation = min(adjusted_kelly, half_kelly)

    return {
        "inputs": {
            "win_rate": win_rate,
            "avg_win_pct": avg_win_pct,
            "avg_loss_pct": avg_loss_pct,
            "account_size": account_size,
            "regime": regime_label,
            "hazard": hazard,
        },
        "kelly_fractions": {
            "full_kelly_pct": round(full_kelly * 100, 2),
            "half_kelly_pct": round(half_kelly * 100, 2),
            "quarter_kelly_pct": round(quarter_kelly * 100, 2),
            "regime_adjusted_pct": round(adjusted_kelly * 100, 2),
            "recommendation_pct": round(recommendation * 100, 2),
        },
        "position_sizes": {
            "full_kelly_usd": round(account_size * full_kelly, 2),
            "half_kelly_usd": round(account_size * half_kelly, 2),
            "recommended_usd": round(account_size * recommendation, 2),
        },
        "adjustments": {
            "regime_multiplier": regime_mult,
            "hazard_multiplier": round(hazard_mult, 3),
        },
        "interpretation": (
            f"Full Kelly: {round(full_kelly * 100, 1)}% exposure. "
            f"In {regime_label} with {hazard}% hazard, "
            f"regime-adjusted recommendation is {round(recommendation * 100, 1)}%. "
            f"Never bet full Kelly — use half or quarter Kelly in practice."
        ),
    }
