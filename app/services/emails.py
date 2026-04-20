import logging
import resend
from app.core.config import settings

logger = logging.getLogger("chainpulse")


def send_email(to: str, subject: str, html: str) -> bool:
    if not settings.RESEND_API_KEY:
        logger.warning("RESEND_API_KEY not set - skipping email")
        return False
    try:
        resend.api_key = settings.RESEND_API_KEY
        resend.Emails.send(
            {
                "from": settings.RESEND_FROM_EMAIL,
                "to": to,
                "subject": subject,
                "html": html,
            }
        )
        return True
    except Exception as e:
        logger.error(f"send_email failed for {to}: {e}")
        return False


def welcome_email_html(email: str, access_token: str) -> str:
    url = f"{settings.FRONTEND_URL}/app?token={access_token}"
    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;
     background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
       letter-spacing:2px;margin-bottom:16px;">ChainPulse Pro</div>
  <h1 style="font-size:24px;margin-bottom:8px;">Your Pro Access Is Active</h1>
  <p style="color:#999;margin-bottom:32px;">
    Click below to open your Pro dashboard.
    This link logs you in automatically. Bookmark it.
  </p>
  <a href="{url}"
     style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
            text-decoration:none;font-weight:bold;border-radius:4px;">
    Open Pro Dashboard
  </a>
  <div style="margin-top:40px;border-top:1px solid #222;padding-top:24px;">
    <p style="color:#555;font-size:12px;margin-bottom:12px;">
      What you now have access to:
    </p>
    <ul style="color:#666;font-size:12px;line-height:2.2;padding-left:16px;">
      <li>Multi-timeframe regime stack - Macro / Trend / Execution</li>
      <li>Exposure recommendation %</li>
      <li>Shift risk % and hazard rate</li>
      <li>Survival probability and curve</li>
      <li>Decision Engine - Daily Directive</li>
      <li>If You Do Nothing simulator</li>
      <li>Regime stress meter and countdown timer</li>
      <li>Volatility and liquidity environment</li>
      <li>Transition probability matrix</li>
      <li>Portfolio exposure allocator</li>
      <li>Exposure logger and discipline score</li>
      <li>Performance comparison vs model</li>
      <li>Edge profile and mistake replay</li>
      <li>Risk profile calibration</li>
      <li>Full cross-asset correlation monitor</li>
      <li>Real-time shift alerts via email</li>
      <li>Daily morning regime brief</li>
      <li>Weekly discipline summary</li>
      <li>Multi-asset: BTC, ETH, SOL, BNB, AVAX, LINK, ADA</li>
    </ul>
  </div>
  <p style="color:#333;font-size:11px;margin-top:40px;
       border-top:1px solid #111;padding-top:20px;">
    ChainPulse. Not financial advice.
  </p>
</div>
"""


def regime_alert_html(coin: str, stack: dict, quality: dict = None) -> str:
    macro_l = stack["macro"]["label"] if stack.get("macro") else "-"
    trend_l = stack["trend"]["label"] if stack.get("trend") else "-"
    exec_l = stack["execution"]["label"] if stack.get("execution") else "-"
    align = stack.get("alignment", 0)
    shift_risk = stack.get("shift_risk", 0)
    exposure = stack.get("exposure", 0)

    from app.utils.enums import PLAYBOOK_DATA

    pb = PLAYBOOK_DATA.get(exec_l, PLAYBOOK_DATA["Neutral"])

    quality_row = ""
    if quality:
        grade_color = (
            "#34d399"
            if quality["grade"].startswith("A")
            else (
                "#4ade80"
                if quality["grade"].startswith("B")
                else "#facc15" if quality["grade"].startswith("C") else "#f87171"
            )
        )
        quality_row = f"""
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
           color:#555;font-size:12px;">Regime Grade</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
           color:{grade_color};text-align:right;font-weight:bold;">
        {quality['grade']} - {quality['structural']}
      </td>
    </tr>"""

    actions_html = "".join(
        f'<li style="color:#999;font-size:12px;line-height:1.8;">{a}</li>'
        for a in pb["actions"][:3]
    )

    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;
     background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
       letter-spacing:2px;margin-bottom:16px;">ChainPulse Alert</div>
  <h2 style="color:#f87171;margin-bottom:16px;">
    Regime Shift Risk Elevated - {coin}
  </h2>
  <table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Macro (1D)</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#fff;text-align:right;">{macro_l}</td>
    </tr>
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Trend (4H)</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#fff;text-align:right;">{trend_l}</td>
    </tr>
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Execution (1H)</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#fff;text-align:right;">{exec_l}</td>
    </tr>
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Alignment</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#fff;text-align:right;">{align}%</td>
    </tr>
    {quality_row}
  </table>
  <table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
    <tr>
      <td style="padding:8px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Shift Risk</td>
      <td style="padding:8px 0;border-bottom:1px solid #1f1f1f;color:#f87171;text-align:right;font-weight:bold;">{shift_risk}%</td>
    </tr>
    <tr>
      <td style="padding:8px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Recommended Exposure</td>
      <td style="padding:8px 0;border-bottom:1px solid #1f1f1f;color:#fff;text-align:right;font-weight:bold;">{exposure}%</td>
    </tr>
    <tr>
      <td style="padding:8px 0;color:#555;font-size:12px;">Strategy</td>
      <td style="padding:8px 0;color:#fff;text-align:right;font-weight:bold;">{pb['strategy_mode']}</td>
    </tr>
  </table>
  <div style="border:1px solid #1f1f1f;padding:16px;margin-bottom:24px;">
    <div style="font-size:11px;color:#555;text-transform:uppercase;
         letter-spacing:1px;margin-bottom:10px;">
      Regime Playbook - {exec_l}
    </div>
    <ul style="padding-left:16px;margin:0;">{actions_html}</ul>
  </div>
  <a href="{settings.FRONTEND_URL}/app"
     style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
            text-decoration:none;font-weight:bold;border-radius:4px;">
    View Dashboard
  </a>
  <p style="color:#333;font-size:11px;margin-top:40px;
       border-top:1px solid #111;padding-top:20px;">
    ChainPulse. Not financial advice.
  </p>
</div>
"""


def morning_email_html(stacks: list, access_token: str) -> str:
    from app.utils.enums import PLAYBOOK_DATA
    from app.services.market_data import compute_regime_quality

    url = (
        f"{settings.FRONTEND_URL}/app?token={access_token}"
        if access_token
        else f"{settings.FRONTEND_URL}/app"
    )
    rows = ""
    for s in stacks:
        shift_risk = s.get("shift_risk") or 0
        exposure = s.get("exposure") or 0
        exec_label = s["execution"]["label"] if s.get("execution") else "-"
        macro_label = s["macro"]["label"] if s.get("macro") else "-"
        pb = PLAYBOOK_DATA.get(exec_label, PLAYBOOK_DATA["Neutral"])
        quality = compute_regime_quality(s)

        risk_color = (
            "#f87171"
            if shift_risk > 70
            else "#facc15" if shift_risk > 45 else "#4ade80"
        )
        grade_color = (
            "#34d399"
            if quality["grade"].startswith("A")
            else (
                "#4ade80"
                if quality["grade"].startswith("B")
                else "#facc15" if quality["grade"].startswith("C") else "#f87171"
            )
        )
        rows += f"""
        <tr>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:#fff;font-weight:600;">{s["coin"]}</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:#999;font-size:12px;">{macro_label}</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:#999;font-size:12px;">{exec_label}</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:#fff;">{exposure}%</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:{risk_color};font-weight:600;">{shift_risk}%</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:{grade_color};font-weight:600;">{quality["grade"]}</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:#666;font-size:11px;">{pb['strategy_mode']}</td>
        </tr>"""

    return f"""
<div style="font-family:sans-serif;max-width:640px;margin:0 auto;
     background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
       letter-spacing:2px;margin-bottom:16px;">ChainPulse Morning Brief</div>
  <h1 style="font-size:22px;margin-bottom:8px;">Daily Regime Snapshot</h1>
  <p style="color:#666;font-size:13px;margin-bottom:32px;">
    Multi-timeframe regime conditions across all tracked assets.
  </p>
  <table style="width:100%;border-collapse:collapse;">
    <thead>
      <tr>
        {"".join(
            f'<th style="text-align:left;padding:8px;color:#444;font-size:11px;text-transform:uppercase;border-bottom:1px solid #222;">{h}</th>'
            for h in ["Asset", "Macro", "Execution", "Exposure", "Shift Risk", "Grade", "Mode"]
        )}
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>
  <div style="margin-top:32px;border:1px solid #1f1f1f;padding:20px;">
    <div style="font-size:11px;color:#555;text-transform:uppercase;
         letter-spacing:1px;margin-bottom:12px;">How to use this brief</div>
    <ul style="color:#666;font-size:12px;line-height:2;padding-left:16px;margin:0;">
      <li>Grade A/B+ = high quality regime - favour continuation trades</li>
      <li>Grade C/D = fragile regime - reduce size, widen stops</li>
      <li>Shift Risk &gt;70% = consider reducing exposure now</li>
      <li>Check dashboard for full playbook and survival curve</li>
    </ul>
  </div>
  <a href="{url}"
     style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
            margin-top:32px;text-decoration:none;font-weight:bold;border-radius:4px;">
    Open Dashboard
  </a>
  <p style="color:#333;font-size:11px;margin-top:40px;
       border-top:1px solid #111;padding-top:20px;">
    ChainPulse. Not financial advice.
  </p>
</div>
"""


def weekly_discipline_email_html(
    email: str, discipline: dict, access_token: str
) -> str:
    url = (
        f"{settings.FRONTEND_URL}/app?token={access_token}"
        if access_token
        else f"{settings.FRONTEND_URL}/app"
    )
    score = discipline.get("score")
    label = discipline.get("label", "-")
    summary = discipline.get("summary", "")
    followed = discipline.get("followed", 0)
    total = discipline.get("total", 0)
    bonuses = discipline.get("bonuses", 0)
    penalties = discipline.get("penalties", 0)
    flags = discipline.get("flags", [])

    score_color = (
        "#34d399"
        if score and score >= 85
        else (
            "#4ade80"
            if score and score >= 70
            else "#facc15" if score and score >= 50 else "#f87171"
        )
    )
    score_display = f"{score}" if score is not None else "N/A"

    flags_html = ""
    for f in flags[-5:]:
        flag_color = "#4ade80" if f["type"] == "bonus" else "#f87171"
        flags_html += f"""
        <tr>
          <td style="padding:8px 0;border-bottom:1px solid #1a1a1a;
               color:{flag_color};font-size:12px;">{f['label']}</td>
          <td style="padding:8px 0;border-bottom:1px solid #1a1a1a;
               color:#555;font-size:11px;text-align:right;">
            {f['date']} - {f['regime']}
          </td>
        </tr>"""

    if not flags_html:
        flags_html = """
        <tr>
          <td colspan="2" style="padding:8px 0;color:#444;font-size:12px;">
            No discipline events recorded this week.
          </td>
        </tr>"""

    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;
     background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
       letter-spacing:2px;margin-bottom:16px;">ChainPulse Weekly Summary</div>
  <h1 style="font-size:22px;margin-bottom:8px;">Your Discipline Report</h1>
  <p style="color:#666;font-size:13px;margin-bottom:32px;">
    Here is how you tracked against the model this week.
  </p>
  <div style="text-align:center;padding:32px;border:1px solid #1f1f1f;margin-bottom:32px;">
    <div style="font-size:48px;font-weight:700;color:{score_color};">
      {score_display}
    </div>
    <div style="font-size:14px;color:{score_color};margin-top:8px;">{label}</div>
    <div style="font-size:12px;color:#555;margin-top:8px;">{summary}</div>
  </div>
  <table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Times Followed Model</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#fff;text-align:right;">{followed} / {total}</td>
    </tr>
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Discipline Bonuses</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#4ade80;text-align:right;">+{bonuses}</td>
    </tr>
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Discipline Penalties</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#f87171;text-align:right;">-{penalties}</td>
    </tr>
  </table>
  <div style="border:1px solid #1f1f1f;padding:16px;margin-bottom:24px;">
    <div style="font-size:11px;color:#555;text-transform:uppercase;
         letter-spacing:1px;margin-bottom:12px;">Recent Discipline Events</div>
    <table style="width:100%;border-collapse:collapse;">{flags_html}</table>
  </div>
  <a href="{url}"
     style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
            text-decoration:none;font-weight:bold;border-radius:4px;">
    View Full Dashboard
  </a>
  <p style="color:#333;font-size:11px;margin-top:40px;
       border-top:1px solid #111;padding-top:20px;">
    ChainPulse. Not financial advice.
  </p>
</div>
"""


def onboarding_day0_html(email: str, access_token: str, stack: dict = None) -> str:
    url = (
        f"{settings.FRONTEND_URL}/app?token={access_token}"
        if access_token
        else f"{settings.FRONTEND_URL}/app"
    )
    regime_line = ""
    directive_line = ""
    if stack and not stack.get("incomplete"):
        exec_label = (
            stack["execution"]["label"] if stack.get("execution") else "Neutral"
        )
        exposure = stack.get("exposure") or 50
        regime_line = f'<p style="color:#fff;font-size:16px;">Current BTC Regime: <strong>{exec_label}</strong></p>'
        directive_line = f'<p style="color:#999;font-size:14px;">Recommended Exposure: <strong style="color:#fff;">{exposure}%</strong></p>'

    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;
     background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
       letter-spacing:2px;margin-bottom:16px;">ChainPulse Pro - Day 0</div>
  <h1 style="font-size:22px;margin-bottom:16px;">
    Welcome! Here's your regime status right now.
  </h1>
  {regime_line}
  {directive_line}
  <p style="color:#999;font-size:13px;margin-top:24px;">
    Your one action for today:
    <strong style="color:#fff;">
      Open your dashboard and check the Decision Engine directive.
    </strong>
    It tells you exactly what to do with your positions right now.
  </p>
  <a href="{url}"
     style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
            margin-top:24px;text-decoration:none;font-weight:bold;border-radius:4px;">
    Open Dashboard
  </a>
  <p style="color:#333;font-size:11px;margin-top:40px;
       border-top:1px solid #111;padding-top:20px;">
    ChainPulse. Not financial advice.
  </p>
</div>
"""


def onboarding_day2_html(email: str, access_token: str) -> str:
    url = (
        f"{settings.FRONTEND_URL}/app?token={access_token}"
        if access_token
        else f"{settings.FRONTEND_URL}/app"
    )
    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;
     background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
       letter-spacing:2px;margin-bottom:16px;">ChainPulse Pro - Day 2</div>
  <h1 style="font-size:22px;margin-bottom:16px;">
    You've been Pro for 48 hours.
  </h1>
  <p style="color:#999;font-size:14px;line-height:1.7;">
    Have you logged your first exposure yet? The
    <strong style="color:#fff;">Exposure Logger</strong>
    tracks your positions against the model's recommendation.
    This builds your
    <strong style="color:#fff;">Discipline Score</strong>
    - the #1 predictor of long-term performance.
  </p>
  <a href="{url}"
     style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
            margin-top:24px;text-decoration:none;font-weight:bold;border-radius:4px;">
    Log Your First Exposure
  </a>
  <p style="color:#333;font-size:11px;margin-top:40px;
       border-top:1px solid #111;padding-top:20px;">
    ChainPulse. Not financial advice.
  </p>
</div>
"""


def onboarding_day5_html(email: str, access_token: str) -> str:
    url = (
        f"{settings.FRONTEND_URL}/app?token={access_token}"
        if access_token
        else f"{settings.FRONTEND_URL}/app"
    )
    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;
     background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
       letter-spacing:2px;margin-bottom:16px;">ChainPulse Pro - Day 5</div>
  <h1 style="font-size:22px;margin-bottom:16px;">
    Your discipline score is building.
  </h1>
  <p style="color:#999;font-size:14px;line-height:1.7;">
    After 5 days, you're building behavioral data. Check your
    <strong style="color:#fff;">Behavioral Alpha</strong> report
    to see if any patterns are costing you money - and how to fix them.
  </p>
  <a href="{url}"
     style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
            margin-top:24px;text-decoration:none;font-weight:bold;border-radius:4px;">
    View Behavioral Insights
  </a>
  <p style="color:#333;font-size:11px;margin-top:40px;
       border-top:1px solid #111;padding-top:20px;">
    ChainPulse. Not financial advice.
  </p>
</div>
"""


def onboarding_day6_html(email: str, access_token: str) -> str:
    url = (
        f"{settings.FRONTEND_URL}/app?token={access_token}"
        if access_token
        else f"{settings.FRONTEND_URL}/app"
    )
    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;
     background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
       letter-spacing:2px;margin-bottom:16px;">ChainPulse Pro - Trial Ending</div>
  <h1 style="font-size:22px;margin-bottom:16px;">
    Your trial ends tomorrow.
  </h1>
  <p style="color:#999;font-size:14px;line-height:1.7;">
    Here's what you'll lose access to:
  </p>
  <ul style="color:#f87171;font-size:13px;line-height:2;padding-left:16px;">
    <li>Decision Engine directives</li>
    <li>Setup Quality &amp; entry timing</li>
    <li>Probabilistic scenarios</li>
    <li>Internal damage monitor</li>
    <li>Behavioral alpha leak detection</li>
    <li>Trade plan generator</li>
    <li>All email alerts &amp; briefs</li>
  </ul>
  <p style="color:#999;font-size:14px;margin-top:16px;">
    Your discipline score, exposure history, and behavioral data
    will be preserved if you continue.
  </p>
  <a href="{settings.FRONTEND_URL}/pricing"
     style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
            margin-top:24px;text-decoration:none;font-weight:bold;border-radius:4px;">
    Keep Pro Access
  </a>
  <p style="color:#333;font-size:11px;margin-top:40px;
       border-top:1px solid #111;padding-top:20px;">
    ChainPulse. Not financial advice.
  </p>
</div>
"""
