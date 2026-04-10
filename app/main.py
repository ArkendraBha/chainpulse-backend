import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.core.logging_middleware import RequestLoggingMiddleware
app.add_middleware(RequestLoggingMiddleware)

from sqlalchemy import text

from app.core.config import settings
from app.core.startup import register_startup_events

from app.routers import public
from app.routers import pro
from app.routers import institutional
from app.routers import dashboards
from app.routers import alerts as alerts_router
from app.routers import performance
from app.routers import user
from app.routers import trade
from app.routers import webhooks as webhooks_router
from app.routers import admin

app = FastAPI(
    title="ChainPulse API",
    version=settings.MODEL_VERSION,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

register_startup_events(app)

app.include_router(public.router)
app.include_router(pro.router)
app.include_router(institutional.router)
app.include_router(dashboards.router)
app.include_router(alerts_router.router)
app.include_router(performance.router)
app.include_router(user.router)
app.include_router(trade.router)
app.include_router(webhooks_router.router)
app.include_router(admin.router)

import traceback
import uuid
from fastapi.responses import JSONResponse
from fastapi import HTTPException as FastAPIHTTPException

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    error_id = str(uuid.uuid4())[:8]
    import logging
    logging.getLogger("chainpulse").error(
        f"Unhandled exception [{error_id}]: "
        f"{type(exc).__name__}: {exc}\n"
        f"{traceback.format_exc()}"
    )
    return JSONResponse(
        status_code=500,
        content={
            "detail": "An internal error occurred.",
            "error_id": error_id,
            "message": (
                f"Please contact support with error ID: {error_id}"
            ),
        },
    )


@app.exception_handler(FastAPIHTTPException)
async def http_exception_handler(
    request: Request, exc: FastAPIHTTPException
):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
        headers=getattr(exc, "headers", None) or {},
    )



@app.get("/health")
def health_check():
    from app.db.database import engine
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {
            "status": "healthy",
            "version": settings.MODEL_VERSION,
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }
    except Exception as e:
        raise HTTPException(503, detail=f"Unhealthy: {e}")


