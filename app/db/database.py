import os
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import NullPool
from app.core.config import settings

DATABASE_URL = settings.DATABASE_URL


def create_db_engine():
    if DATABASE_URL.startswith("sqlite"):
        return create_engine(
            DATABASE_URL,
            connect_args={"check_same_thread": False},
        )

    # Neon / Supabase serverless - use NullPool
    is_serverless = any(x in DATABASE_URL for x in [
        "neon.tech", "supabase", "pooler"
    ])

    if is_serverless:
        return create_engine(
            DATABASE_URL,
            poolclass=NullPool,
            connect_args={
                "sslmode": "require",
                "connect_timeout": 10,
                "application_name": "chainpulse",
            },
        )

    # Standard PostgreSQL
    return create_engine(
        DATABASE_URL,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=300,
        pool_timeout=30,
        connect_args={"connect_timeout": 10},
    )


engine = create_db_engine()
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
