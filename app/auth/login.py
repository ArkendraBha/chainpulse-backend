import uuid
import datetime
from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.db.models import User
from app.auth.auth import generate_access_token, hash_token
from app.core.config import settings

def create_login_session(email: str, db: Session) -> dict:
    """
    Creates a login session for a user.
    Returns login URL for email.
    """
    user = db.query(User).filter(User.email == email).first()
    if not user or user.subscription_status not in ("active", "trialing"):
        raise HTTPException(404, detail="No active subscription found")

    # Generate new token
    raw_token = generate_access_token()
    user.access_token = hash_token(raw_token)
    user.token_created_at = datetime.datetime.utcnow()
    db.commit()

    return {
        "login_url": f"{settings.FRONTEND_URL}/app?token={raw_token}",
        "expires_in": f"{settings.TOKEN_EXPIRY_DAYS} days",
    }

def send_login_email(email: str, db: Session) -> bool:
    """
    Sends a login link email to the user.
    """
    from app.services.emails import send_email
    
    login_data = create_login_session(email, db)
    
    html = f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;letter-spacing:2px;margin-bottom:16px;">ChainPulse Login</div>
  <h1 style="font-size:24px;margin-bottom:16px;">Your Login Link</h1>
  <p style="color:#999;font-size:14px;margin-bottom:32px;">
    Click the button below to securely access your ChainPulse dashboard.
    This link expires in {settings.TOKEN_EXPIRY_DAYS} days.
  </p>
  <a href="{login_data['login_url']}" 
     style="display:inline-block;background:#10b981;color:#fff;padding:16px 32px;text-decoration:none;font-weight:bold;border-radius:12px;margin-bottom:24px;">
    Access Dashboard ?
  </a>
  <p style="color:#666;font-size:12px;">
    For security, this link only works once. Request a new one if needed.
  </p>
  <p style="color:#333;font-size:11px;margin-top:40px;border-top:1px solid #111;padding-top:20px;">
    ChainPulse. Not financial advice.
  </p>
</div>
"""
    
    return send_email(
        email,
        "ChainPulse - Your Secure Login Link",
        html
    )
