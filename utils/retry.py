import functools
import time
import random
from utils.logger import get_logger

log = get_logger("retry")


def retry(max_attempts: int = 3, base_delay: float = 2.0, max_delay: float = 30.0):
    """Exponential backoff with jitter. Retries on any Exception."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:
                    if attempt == max_attempts:
                        raise
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    jitter = random.uniform(0, delay * 0.25)
                    wait = delay + jitter
                    log.warning(
                        f"{fn.__name__} attempt {attempt}/{max_attempts} failed: {exc}. "
                        f"Retrying in {wait:.1f}s…"
                    )
                    time.sleep(wait)
        return wrapper
    return decorator
