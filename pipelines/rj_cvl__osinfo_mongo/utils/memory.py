"""Memory guard for the upload phase of rj_cvl__osinfo_mongo.

Turning one file's chunks into a parquet plus a rebuilt PDF peaks at roughly
8-10x the file size on top of the chunks already held in memory. A few large
PDFs converted at the same time are enough to exceed the pod limit, so the
conversions share a byte budget instead of running unbounded.
"""

import threading

# Peak transient memory per file byte while converting (measured: ~8-10x).
PEAK_FACTOR = 10
# Assumed size when a file's length is unknown.
DEFAULT_FILE_BYTES = 2 * 1024 * 1024
# Combined weight allowed in flight across all concurrent uploads.
MEMORY_BUDGET_BYTES = 2 * 1024**3


class ByteBudget:
    """Caps the combined weight of work in flight across threads.

    An item heavier than the whole budget still runs, alone, so nothing waits forever.
    """

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self._used = 0
        self._cond = threading.Condition()

    def acquire(self, weight: int) -> None:
        """Block until ``weight`` fits in the budget (or nothing else is running)."""
        with self._cond:
            while self._used and self._used + weight > self.capacity:
                self._cond.wait()
            self._used += weight

    def release(self, weight: int) -> None:
        """Return ``weight`` to the budget and wake up waiting threads."""
        with self._cond:
            self._used -= weight
            self._cond.notify_all()


def upload_weight(file_length: int | None) -> int:
    """Budget weight of converting and uploading one file.

    Args:
        file_length: File size in bytes (GridFS ``length``), or None if unknown.

    Returns:
        Estimated peak transient bytes.
    """
    return (file_length or DEFAULT_FILE_BYTES) * PEAK_FACTOR


UPLOAD_BUDGET = ByteBudget(MEMORY_BUDGET_BYTES)
