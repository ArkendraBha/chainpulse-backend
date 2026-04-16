import uuid
from fastapi import Request
from fastapi.responses import JSONResponse


class AppError(Exception):
    def __init__(
        self,
        message: str,
        code: str,
        status_code: int = 400,
        details: dict = None,
    ):
        self.message = message
        self.code = code
        self.status_code = status_code
        self.details = details or {}
        super().__init__(message)


class TierError(AppError):
    def __init__(self, required_tier: str, current_tier: str):
        super().__init__(
            message=f"This feature requires {required_tier} tier",
            code="TIER_REQUIRED",
            status_code=403,
            details={
                "required_tier": required_tier,
                "current_tier": current_tier,
                "upgrade_url": "/pricing",
            },
        )


class ValidationError(AppError):
    def __init__(self, field: str, message: str):
        super().__init__(
            message=message,
            code="VALIDATION_ERROR",
            status_code=422,
            details={"field": field},
        )
