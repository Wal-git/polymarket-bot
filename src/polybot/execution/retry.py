import time
from functools import wraps
from typing import Callable

import structlog

logger = structlog.get_logger()


class CircuitOpen(Exception):
    pass


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 60.0):
        self._failure_count = 0
        self._threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._last_failure_time: float = 0
        self._is_open = False

    def record_success(self):
        self._failure_count = 0
        self._is_open = False

    def record_failure(self):
        self._failure_count += 1
        self._last_failure_time = time.time()
        if self._failure_count >= self._threshold:
            self._is_open = True
            logger.warning("circuit_opened", failures=self._failure_count)

    def allow_request(self) -> bool:
        if not self._is_open:
            return True
        if time.time() - self._last_failure_time > self._reset_timeout:
            self._is_open = False
            self._failure_count = 0
            logger.info("circuit_reset")
            return True
        return False


def with_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    circuit: CircuitBreaker | None = None,
):
    def decorator(fn: Callable):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if circuit and not circuit.allow_request():
                raise CircuitOpen(f"Circuit open for {fn.__name__}")

            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    result = fn(*args, **kwargs)
                    if circuit:
                        circuit.record_success()
                    return result
                except Exception as e:
                    last_exc = e
                    if circuit:
                        circuit.record_failure()
                    if attempt < max_attempts:
                        delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                        logger.warning(
                            "retry",
                            fn=fn.__name__,
                            attempt=attempt,
                            delay=delay,
                            error=str(e),
                        )
                        time.sleep(delay)

            raise last_exc  # type: ignore

        return wrapper
    return decorator
