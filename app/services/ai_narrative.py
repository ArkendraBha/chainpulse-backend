# app/services/ai_narrative.py
import openai
import hashlib
from app.core.config import settings
from app.core.cache import cache_get, cache_set

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


async def generate_regime_narrative(
    coin: str,
    stack: dict,
    setup: dict,
    scenarios: dict,
    damage: dict,
) -> dict:
    """
    Generates a GPT-4 powered regime narrative.
    Cached for 1 hour per coin to minimize API costs.
    """
    if not OPENAI_API_KEY:
        return {"available": False, "reason": "AI not configured"}

    # Cache key based on regime state (not time)
    exec_label = (
        stack["execution"]["label"]
        if stack.get("execution") else "Neutral"
    )
    cache_key = f"narrative:{coin}:{exec_label}:{int(stack.get('hazard', 0))}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    # Build context for GPT
    context = f"""
You are ChainPulse AI, an institutional crypto market analyst.
Analyze the following regime data and write a professional market brief.

COIN: {coin}
MACRO REGIME: {stack.get('macro', {}).get('label', 'N/A')} 
TREND REGIME: {stack.get('trend', {}).get('label', 'N/A')}
EXECUTION REGIME: {exec_label}
ALIGNMENT: {stack.get('alignment', 0)}%
HAZARD RATE: {stack.get('hazard', 0)}%
SURVIVAL PROBABILITY: {stack.get('survival', 0)}%
SHIFT RISK: {stack.get('shift_risk', 0)}%
RECOMMENDED EXPOSURE: {stack.get('exposure', 0)}%
SETUP QUALITY: {setup.get('setup_quality_score', 'N/A')}
CHASE RISK: {setup.get('chase_risk', 'N/A')}
INTERNAL DAMAGE SCORE: {damage.get('internal_damage_score', 'N/A')}
BASE CASE ({scenarios.get('scenarios', [{}])[0].get('probability', 0)}%): {scenarios.get('scenarios', [{}])[0].get('outcome', 'N/A')}

Write exactly 3 paragraphs:
1. Current regime assessment and what it means for traders (2-3 sentences)
2. Key risks and what to watch (2-3 sentences)  
3. Specific actionable guidance for the next 24-48 hours (2-3 sentences)

Be direct, data-driven, and institutional in tone. No hedging. No disclaimers.
"""

    try:
        client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)
        response = await client.chat.completions.create(
            model="gpt-4o-mini",  # Fast and cheap - $0.00015/1K tokens
            messages=[
                {
                    "role": "system",
                    "content": "You are an institutional crypto market analyst. Be concise, direct, and data-driven."
                },
                {
                    "role": "user",
                    "content": context,
                }
            ],
            max_tokens=400,
            temperature=0.3,  # Low temperature for consistency
        )

        narrative = response.choices[0].message.content
        result = {
            "available": True,
            "narrative": narrative,
            "model": "gpt-4o-mini",
            "coin": coin,
            "regime_context": exec_label,
            "generated_at": datetime.datetime.utcnow().isoformat(),
        }

        # Cache for 1 hour
        cache_set(cache_key, result, ttl=3600)
        return result

    except Exception as e:
        logger.error(f"AI narrative generation failed: {e}")
        return {
            "available": False,
            "reason": "AI generation failed",
            "error": str(e),
        }


# Add to premium-dashboard endpoint:
# narrative = await generate_regime_narrative(coin, stack, setup, scenarios, damage)
# result["ai_narrative"] = narrative
