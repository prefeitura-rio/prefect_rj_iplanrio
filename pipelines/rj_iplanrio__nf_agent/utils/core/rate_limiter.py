"""
Global rate limiter using threading primitives.
Thread-safe implementation for controlling API request rate.
"""
import logging
import threading
import time
from collections import deque

logger = logging.getLogger(__name__)


class GlobalRateLimiter:
    """
    Global rate limiter using semaphore and sliding window.
    Thread-safe implementation for synchronous code.

    Controls:
    - Max concurrent requests (semaphore)
    - Requests per minute (sliding window)

    Usage:
        limiter = get_rate_limiter(max_concurrent=50, requests_per_minute=600)

        limiter.acquire()
        try:
            # Make API call
            response = api_call()
        finally:
            limiter.release()
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, max_concurrent: int = 50, requests_per_minute: int = 600):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, max_concurrent: int = 50, requests_per_minute: int = 600):
        if self._initialized:
            # Allow reconfiguration if parameters changed
            if (self.max_concurrent != max_concurrent or self.rpm_limit != requests_per_minute):
                logger.warning(
                    f"[RateLimiter] Attempting to reconfigure singleton from "
                    f"({self.max_concurrent}, {self.rpm_limit}) to "
                    f"({max_concurrent}, {requests_per_minute}). "
                    f"Using existing configuration."
                )
            return

        self.max_concurrent = max_concurrent
        self.rpm_limit = requests_per_minute
        self.enabled = True  # Can be disabled for testing

        self.semaphore = threading.Semaphore(max_concurrent)
        self.request_times = deque()
        self.times_lock = threading.Lock()

        self._initialized = True

        logger.info(
            f"[RateLimiter] Initialized with max_concurrent={max_concurrent}, "
            f"rpm={requests_per_minute} ({requests_per_minute/60:.1f} RPS)"
        )

    def acquire(self):
        """
        Acquire permission to make a request.
        Blocks if limits are exceeded.
        """
        if not self.enabled:
            return

        # Wait for semaphore (max concurrent limit)
        self.semaphore.acquire()

        # Wait for RPM limit (sliding window)
        self._wait_for_rpm_limit()

        # Record start time NOW — after slot acquired but before API call begins.
        # Must be here (not in release) so the window reflects in-flight requests.
        with self.times_lock:
            self.request_times.append(time.time())

    def release(self):
        """Release permission after request completes"""
        if not self.enabled:
            return

        # Release semaphore
        self.semaphore.release()

    def _wait_for_rpm_limit(self):
        """Block if we've hit the RPM limit (sliding window)"""
        while True:
            with self.times_lock:
                now = time.time()

                # Remove requests older than 1 minute
                while self.request_times and now - self.request_times[0] > 60:
                    self.request_times.popleft()

                # Check if we can proceed
                if len(self.request_times) < self.rpm_limit:
                    return

                # Calculate how long to wait
                oldest_request = self.request_times[0]
                wait_time = 60 - (now - oldest_request)

            # Rate limit exceeded, wait a bit
            if wait_time > 0:
                time.sleep(min(wait_time, 0.1))  # Wait at most 100ms at a time
            else:
                time.sleep(0.01)  # Small sleep to avoid busy-wait

    def get_current_rate(self) -> float:
        """Get current requests per minute (last 60 seconds)"""
        with self.times_lock:
            now = time.time()
            # Remove old requests
            while self.request_times and now - self.request_times[0] > 60:
                self.request_times.popleft()

            return len(self.request_times)

    def get_current_rps(self) -> float:
        """Get current requests per second (last 60 seconds average)"""
        return self.get_current_rate() / 60.0

    def reset(self):
        """Reset rate limiter state (for testing)"""
        with self.times_lock:
            self.request_times.clear()

    def set_enabled(self, enabled: bool):
        """Enable or disable rate limiting"""
        self.enabled = enabled
        if enabled:
            logger.info("[RateLimiter] Enabled")
        else:
            logger.warning("[RateLimiter] Disabled - no rate limiting applied")


# Global singleton (lazy initialization)
_rate_limiter: GlobalRateLimiter | None = None
_limiter_lock = threading.Lock()


def get_rate_limiter(
    max_concurrent: int = 50,
    requests_per_minute: int = 600
) -> GlobalRateLimiter:
    """
    Get or create the global rate limiter.

    :param max_concurrent: Maximum concurrent API requests (default: 50).
    :param requests_per_minute: Maximum requests per minute (default: 600 = 10 RPS).
    :returns: GlobalRateLimiter singleton instance.

    Note:
        The rate limiter is a singleton. If it's already initialized,
        the parameters are ignored and the existing instance is returned.
    """
    global _rate_limiter
    if _rate_limiter is None:
        with _limiter_lock:
            if _rate_limiter is None:
                _rate_limiter = GlobalRateLimiter(max_concurrent, requests_per_minute)
    return _rate_limiter


def reset_rate_limiter():
    """Reset the global rate limiter (for testing)"""
    global _rate_limiter
    with _limiter_lock:
        _rate_limiter = None


def initialize_rate_limiter(
    max_concurrent: int = 50,
    requests_per_minute: int = 600,
    force: bool = False
) -> GlobalRateLimiter:
    """
    Initialize the global rate limiter with specific parameters.

    MUST be called BEFORE any other code imports GeminiClassifier or NFExtractor.

    :param max_concurrent: Maximum concurrent API requests.
    :param requests_per_minute: Maximum requests per minute.
    :param force: If True, reset and recreate with new parameters.
    :returns: GlobalRateLimiter instance.
    """
    global _rate_limiter

    if force and _rate_limiter is not None:
        logger.warning("[RateLimiter] Forcing reset and recreation with new parameters")
        reset_rate_limiter()

    with _limiter_lock:
        if _rate_limiter is None:
            _rate_limiter = GlobalRateLimiter(max_concurrent, requests_per_minute)
        elif (_rate_limiter.max_concurrent != max_concurrent or
              _rate_limiter.rpm_limit != requests_per_minute):
            logger.error(
                f"[RateLimiter] ERROR: Rate limiter already initialized with different params!\n"
                f"  Existing: max_concurrent={_rate_limiter.max_concurrent}, rpm={_rate_limiter.rpm_limit}\n"
                f"  Requested: max_concurrent={max_concurrent}, rpm={requests_per_minute}\n"
                f"  Using existing configuration. Call initialize_rate_limiter() BEFORE importing any modules!"
            )

    return _rate_limiter
