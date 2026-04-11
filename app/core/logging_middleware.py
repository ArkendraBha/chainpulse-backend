import uuid
import time
import logging
import json
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from contextvars import ContextVar

request_id_var: ContextVar[str] = ContextVar(
    "request_id", default="unknown"
)

logger = logging.getLogger("chainpulse")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get(
            "x-request-id", str(uuid.uuid4())[:8]
        )
        request_id_var.set(request_id)
        start = time.perf_counter()

        response = await call_next(request)

        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time"] = f"{duration_ms}ms"

        logger.info(json.dumps({
            "type": "request",
            "method": request.method,
            "path": request.url.path,
            "status": response.status_code,
            "duration_ms": duration_ms,
            "request_id": request_id,
        }))
        sensitive = [
            "/log-exposure",
            "/log-performance",
            "/trade-plan",
            "/api/v1/keys",
            "/stripe-webhook",
            "/save-archetype",
            "/restore-access",
            "/create-checkout-session",
        ]
        if any(request.url.path.startswith(s) for s in sensitive):
            try:
                from app.db.database import SessionLocal
                from app.db.models import AuditLog
                forwarded = request.headers.get("x-forwarded-for")
                ip = (
                    forwarded.split(",")[0].strip()
                    if forwarded else "unknown"
                )
                db = SessionLocal()
                db.add(AuditLog(
                    action=request.method,
                    endpoint=request.url.path,
                    ip_address=ip,
                    details=f"status={response.status_code} request_id={request_id}",
                ))
                db.commit()
                db.close()
            except Exception:
                pass
        return response
