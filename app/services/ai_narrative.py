import os
import datetime
import logging
from app.core.cache import cache_get, cache_set

logger = logging.getLogger("chainpulse")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


async def generate_regime_narrative(
    coin: str,
    stack: dict,
    setup: dict = None,
    scenarios: dict = None,
    damage: dict = None,
) -> dict:
    """
    Generates a GPT-4o-mini powered regime narrative.
    Cached for 1 hour per coin+regime combination.
    Cost: ~$0.0002 per call at current pricing.
    """
    if not OPENAI_API_KEY:
        return {
            "available": False,
            "reason": "OpenAI API key not configured",
        }

    exec_label = (
        stack["execution"]["label"]
        if stack.get("execution") else "Neutral"
    )
    hazard = stack.get("hazard") or 50
    shift_risk = stack.get("shift_risk") or 50
    exposure = stack.get("exposure") or 50
    alignment = stack.get("alignment") or 50
    survival = stack.get("survival") or 50
    direction = stack.get("direction") or "mixed"

    # Cache key based on regime state not time
    # so we dont regenerate if nothing changed
    cache_key = (
        f"narrative:{coin}:{exec_label}:"
        f"{int(hazard)}:{int(shift_risk)}:{int(alignment)}"
    )
    cached = cache_get(cache_key)
    if cached:
        return cached

    setup_score = setup.get("setup_quality_score") if setup else None
    chase_risk = setup.get("chase_risk") if setup else None
    entry_mode = setup.get("entry_mode") if setup else None
    base_scenario = (
        scenarios.get("scenarios", [{}])[0].get("outcome")
        if scenarios else None
    )
    damage_score = damage.get("internal_damage_score") if damage else None
    damage_label = damage.get("damage_label") if damage else None

    macro_label = (
        stack["macro"]["label"] if stack.get("macro") else "Unknown"
    )
    trend_label = (
        stack["trend"]["label"] if stack.get("trend") else "Unknown"
    )

    context = f"""You are ChainPulse AI, an institutional crypto market analyst.
Analyze the following quantitative regime data and write a concise professional market brief.

ASSET: {coin}
MACRO REGIME (1D): {macro_label}
TREND REGIME (4H): {trend_label}
EXECUTION REGIME (1H): {exec_label}
DIRECTION: {direction}
ALIGNMENT: {alignment}% (timeframe agreement)
HAZARD RATE: {hazard}% (regime failure risk)
SURVIVAL PROBABILITY: {survival}%
SHIFT RISK: {shift_risk}% (deterioration signal)
RECOMMENDED EXPOSURE: {exposure}%
{f"SETUP QUALITY: {setup_score}/100" if setup_score is not None else ""}
{f"CHASE RISK: {chase_risk}%" if chase_risk is not None else ""}
{f"ENTRY MODE: {entry_mode}" if entry_mode else ""}
{f"BASE CASE SCENARIO: {base_scenario}" if base_scenario else ""}
{f"INTERNAL DAMAGE: {damage_score}/100 ({damage_label})" if damage_score is not None else ""}

Write exactly 3 paragraphs. Be direct, data-driven, institutional in tone.
No hedging. No disclaimers. No bullet points.

Paragraph 1: Current regime assessment — what the data says and what it means for traders right now.
Paragraph 2: Key risks — what signals are elevated, what to watch, what could invalidate the regime.
Paragraph 3: Specific actionable guidance for the next 24-48 hours based on the data above.
"""

    try:
        import openai
        client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)

        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an institutional crypto market analyst. "
                        "Be concise, direct, and data-driven. "
                        "Write in clear paragraphs. No bullet points. No disclaimers."
                    ),
                },
                {
                    "role": "user",
                    "content": context,
                },
            ],
            max_tokens=450,
            temperature=0.25,
        )

        narrative_text = response.choices[0].message.content

        result = {
            "available": True,
            "narrative": narrative_text,
            "model": "gpt-4o-mini",
            "coin": coin,
            "regime_context": {
                "macro": macro_label,
                "trend": trend_label,
                "execution": exec_label,
                "hazard": hazard,
                "shift_risk": shift_risk,
            },
            "generated_at": datetime.datetime.utcnow().isoformat(),
        }

        # Cache for 1 hour
        cache_set(cache_key, result, ttl=3600)
        return result

    except Exception as e:
        logger.error(f"AI narrative generation failed for {coin}: {e}")
        return {
            "available": False,
            "reason": "Generation failed",
            "error": str(e),
        }


async def generate_daily_intelligence_brief(
    stacks: list,
    what_changed: dict,
) -> dict:
    """
    Generates a daily market intelligence brief covering all coins.
    Used by the morning email and what-changed endpoint.
    """
    if not OPENAI_API_KEY:
        return {"available": False}

    cache_key = f"daily_brief:{datetime.datetime.utcnow().strftime('%Y-%m-%d')}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    coin_summaries = []
    for stack in stacks[:5]:
        if stack.get("incomplete"):
            continue
        coin = stack.get("coin", "Unknown")
        exec_label = (
            stack["execution"]["label"]
            if stack.get("execution") else "Neutral"
        )
        shift_risk = stack.get("shift_risk") or 0
        exposure = stack.get("exposure") or 0
        coin_summaries.append(
            f"{coin}: {exec_label} | Shift Risk {shift_risk}% | "
            f"Exposure {exposure}%"
        )

    headline = what_changed.get("headline", "No major changes")
    tone = what_changed.get("tone", "stable")
    changes_count = what_changed.get("change_count", 0)

    context = f"""You are ChainPulse AI writing the daily market intelligence brief.

MARKET SUMMARY:
{chr(10).join(coin_summaries)}

24H CHANGES: {headline}
MARKET TONE: {tone}
REGIME CHANGES: {changes_count}

Write a 2-paragraph daily brief:
Paragraph 1: Overall market regime summary — what the data shows across all tracked assets.
Paragraph 2: Key focus points for traders today — what to watch and what to act on.

Be direct and institutional. No disclaimers.
"""

    try:
        import openai
        client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)

        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are an institutional crypto market analyst writing a daily brief.",
                },
                {"role": "user", "content": context},
            ],
            max_tokens=300,
            temperature=0.2,
        )

        result = {
            "available": True,
            "brief": response.choices[0].message.content,
            "generated_at": datetime.datetime.utcnow().isoformat(),
        }

        cache_set(cache_key, result, ttl=3600 * 6)
        return result

    except Exception as e:
        logger.error(f"Daily brief generation failed: {e}")
        return {"available": False, "error": str(e)}
