import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
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


