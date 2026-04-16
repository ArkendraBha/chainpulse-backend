import time
import logging
from enum import Enum

logger = logging.getLogger("chainpulse")


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """
    Circuit breaker for external API calls.
    Prevents cascading failures when Binance is down.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED

    def call_succeeded(self):
        self.failure_count = 0
        self.state = CircuitState.CLOSED

    def call_failed(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(
                f"Circuit breaker OPEN for {self.name} "
                f"after {self.failure_count} failures"
            )

    def can_attempt(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if (
                self.last_failure_time
                and time.time() - self.last_failure_time
                > self.recovery_timeout
            ):
                self.state = CircuitState.HALF_OPEN
                logger.info(
                    f"Circuit breaker HALF_OPEN for {self.name}"
                )
                return True
            return False
        return True

    def get_status(self) -> dict:
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "last_failure_time": self.last_failure_time,
        }


binance_circuit = CircuitBreaker(
    "binance", failure_threshold=5, recovery_timeout=60
)
binance_us_circuit = CircuitBreaker(
    "binance_us", failure_threshold=5, recovery_timeout=60
)
