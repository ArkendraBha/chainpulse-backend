# -----------------------------------------
# All constants, enums, playbook data,
# archetype configs, risk events, leak types
# -----------------------------------------

RISK_EVENTS = [
    {"name": "FOMC Meeting", "type": "macro", "impact": "High"},
    {"name": "CPI Release", "type": "macro", "impact": "High"},
    {"name": "Options Expiry", "type": "market", "impact": "Medium"},
    {"name": "ETF Flow Report", "type": "market", "impact": "Medium"},
    {"name": "BTC Halving", "type": "crypto", "impact": "High"},
    {"name": "Fed Minutes", "type": "macro", "impact": "Medium"},
    {"name": "PCE Inflation", "type": "macro", "impact": "High"},
]

DYNAMIC_RISK_EVENTS = [
    {
        "name": "FOMC Meeting",
        "type": "macro",
        "impact": "High",
        "recurrence": "6_weeks",
        "typical_vol_multiplier": 1.8,
        "regime_survival_impact": -15,
    },
    {
        "name": "CPI Release",
        "type": "macro",
        "impact": "High",
        "recurrence": "monthly",
        "typical_vol_multiplier": 1.6,
        "regime_survival_impact": -12,
    },
    {
        "name": "Options Expiry",
        "type": "market",
        "impact": "Medium",
        "recurrence": "monthly",
        "typical_vol_multiplier": 1.3,
        "regime_survival_impact": -8,
    },
    {
        "name": "ETF Flow Report",
        "type": "market",
        "impact": "Medium",
        "recurrence": "weekly",
        "typical_vol_multiplier": 1.1,
        "regime_survival_impact": -5,
    },
    {
        "name": "PCE Inflation",
        "type": "macro",
        "impact": "High",
        "recurrence": "monthly",
        "typical_vol_multiplier": 1.5,
        "regime_survival_impact": -10,
    },
    {
        "name": "Fed Minutes",
        "type": "macro",
        "impact": "Medium",
        "recurrence": "6_weeks",
        "typical_vol_multiplier": 1.3,
        "regime_survival_impact": -8,
    },
    {
        "name": "Jobs Report (NFP)",
        "type": "macro",
        "impact": "High",
        "recurrence": "monthly",
        "typical_vol_multiplier": 1.5,
        "regime_survival_impact": -12,
    },
    {
        "name": "Quarterly GDP",
        "type": "macro",
        "impact": "Medium",
        "recurrence": "quarterly",
        "typical_vol_multiplier": 1.2,
        "regime_survival_impact": -6,
    },
]

PLAYBOOK_DATA = {
    "Strong Risk-On": {
        "exposure_band": "65-80%",
        "strategy_mode": "Aggressive",
        "trend_follow_wr": 72,
        "mean_revert_wr": 38,
        "avg_remaining_days": 14,
        "data_source": "backtested_estimates",
        "actions": [
            "Favour trend continuation entries",
            "Pyramiding into strength is valid",
            "Tight stops - volatility is compressed",
            "Hold winners longer than feels comfortable",
        ],
        "avoid": ["Shorting into strength", "Waiting for deep pullbacks"],
    },
    "Risk-On": {
        "exposure_band": "50-65%",
        "strategy_mode": "Balanced",
        "trend_follow_wr": 63,
        "mean_revert_wr": 44,
        "avg_remaining_days": 9,
        "data_source": "backtested_estimates",
        "actions": [
            "Favour pullback entries in trend direction",
            "Scale into positions over 2-3 entries",
            "Monitor breadth for continuation signal",
        ],
        "avoid": [
            "Over-leveraging at breakouts",
            "Chasing extended moves",
        ],
    },
    "Neutral": {
        "exposure_band": "25-45%",
        "strategy_mode": "Neutral",
        "trend_follow_wr": 49,
        "mean_revert_wr": 51,
        "avg_remaining_days": 6,
        "data_source": "backtested_estimates",
        "actions": [
            "Reduce overall exposure",
            "Preserve capital - this is a transition zone",
        ],
        "avoid": ["Strong directional bias", "Large position sizes"],
    },
    "Risk-Off": {
        "exposure_band": "10-25%",
        "strategy_mode": "Defensive",
        "trend_follow_wr": 31,
        "mean_revert_wr": 57,
        "avg_remaining_days": 7,
        "data_source": "backtested_estimates",
        "actions": [
            "Reduce long exposure significantly",
            "Hold cash - optionality has value",
        ],
        "avoid": [
            "Buying dips aggressively",
            "Adding to losing longs",
        ],
    },
    "Strong Risk-Off": {
        "exposure_band": "0-10%",
        "strategy_mode": "Fully Defensive",
        "trend_follow_wr": 22,
        "mean_revert_wr": 48,
        "avg_remaining_days": 11,
        "data_source": "backtested_estimates",
        "actions": [
            "Move to maximum cash allocation",
            "Monitor for capitulation signals",
        ],
        "avoid": [
            "Catching falling knives",
            "Any leveraged long exposure",
        ],
    },
}

ARCHETYPE_CONFIG = {
    "swing": {
        "label": "Swing Trader",
        "exposure_mult": 1.0,
        "alert_sensitivity": "medium",
        "preferred_timeframe": "4h",
        "max_hold_days": 14,
        "stop_width_mult": 1.0,
        "typical_tranches": [30, 30, 20],
        "playbook_bias": "trend_follow",
        "description": "Holds positions for days to weeks. Follows intermediate trends.",
    },
    "position": {
        "label": "Position Trader",
        "exposure_mult": 0.85,
        "alert_sensitivity": "low",
        "preferred_timeframe": "1d",
        "max_hold_days": 60,
        "stop_width_mult": 1.5,
        "typical_tranches": [25, 25, 25, 15],
        "playbook_bias": "macro_follow",
        "description": "Longer-term conviction trades. Macro regime driven.",
    },
    "spot_allocator": {
        "label": "Spot Allocator",
        "exposure_mult": 0.75,
        "alert_sensitivity": "low",
        "preferred_timeframe": "1d",
        "max_hold_days": 90,
        "stop_width_mult": 2.0,
        "typical_tranches": [20, 20, 20, 20],
        "playbook_bias": "buy_and_hold",
        "description": "DCA-oriented. Uses regime data for timing allocation size.",
    },
    "tactical": {
        "label": "Tactical De-risker",
        "exposure_mult": 1.1,
        "alert_sensitivity": "high",
        "preferred_timeframe": "1h",
        "max_hold_days": 7,
        "stop_width_mult": 0.8,
        "typical_tranches": [35, 35, 20],
        "playbook_bias": "mean_revert",
        "description": "Active risk management. Quickly adjusts exposure to regime changes.",
    },
    "leverage": {
        "label": "Leverage Trader",
        "exposure_mult": 1.3,
        "alert_sensitivity": "high",
        "preferred_timeframe": "1h",
        "max_hold_days": 5,
        "stop_width_mult": 0.6,
        "typical_tranches": [40, 30, 20],
        "playbook_bias": "momentum",
        "description": "Uses leverage. Needs tightest risk controls and fastest alerts.",
    },
}

LEAK_TYPES = {
    "late_entry_chasing": {
        "label": "Late Entry / Chasing",
        "description": "Entering positions after significant extension, when chase risk is high.",
        "severity_weight": 1.5,
    },
    "overexposed_risk_off": {
        "label": "Over-Exposed in Risk-Off",
        "description": "Maintaining high exposure when regime signals defensive positioning.",
        "severity_weight": 2.0,
    },
    "ignored_hazard_spike": {
        "label": "Ignored Hazard Spike",
        "description": "Failed to reduce exposure when hazard rate spiked above 65%.",
        "severity_weight": 1.8,
    },
    "premature_exit_strength": {
        "label": "Premature Exit in Strength",
        "description": "Reducing exposure during strong, healthy regime conditions.",
        "severity_weight": 1.0,
    },
    "averaging_down_risk_off": {
        "label": "Averaging Down in Risk-Off",
        "description": "Adding to losing positions during deteriorating regime.",
        "severity_weight": 2.5,
    },
    "overtrading": {
        "label": "Overtrading",
        "description": "Logging exposure changes too frequently relative to regime changes.",
        "severity_weight": 1.2,
    },
    "size_too_large": {
        "label": "Position Size Too Large",
        "description": "Exposure consistently exceeds model recommendation by >25%.",
        "severity_weight": 1.8,
    },
    "failed_to_press_edge": {
        "label": "Failed to Press Edge",
        "description": "Under-exposed during high-quality regime conditions where user has historical edge.",
        "severity_weight": 1.0,
    },
}
