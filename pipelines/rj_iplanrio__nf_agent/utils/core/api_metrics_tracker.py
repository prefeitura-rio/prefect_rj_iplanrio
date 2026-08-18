"""
Módulo para rastrear métricas de API calls (taxa de requests, latência, erros).
Thread-safe tracking para uso em ambiente multi-threaded.
"""
import threading
import time
from dataclasses import dataclass


@dataclass
class APICallRecord:
    """Record of a single API call."""

    timestamp: float
    api_type: str  # 'classification' or 'extraction'
    duration_ms: float
    success: bool
    error_type: str | None = None
    thread_id: int = 0


class APIMetricsTracker:
    """
    Global singleton tracker for API call metrics.
    Thread-safe tracking of requests, rates, latency, and errors.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._data_lock = threading.Lock()
        self.records: list[APICallRecord] = []
        self.errors_429 = []  # Special tracking for 429 errors
        self.start_time = time.time()
        self._initialized = True

    def record_call(
        self,
        api_type: str,
        duration_ms: float,
        success: bool = True,
        error_type: str | None = None,
    ):
        """Record an API call."""
        record = APICallRecord(
            timestamp=time.time(),
            api_type=api_type,
            duration_ms=duration_ms,
            success=success,
            error_type=error_type,
            thread_id=threading.current_thread().ident
        )

        with self._data_lock:
            self.records.append(record)

            # Track 429 errors specially
            if error_type and "429" in str(error_type):
                self.errors_429.append(record)

    def get_metrics(self) -> dict:
        """Calculate and return all metrics."""
        with self._data_lock:
            if not self.records:
                return self._empty_metrics()

            total_duration = time.time() - self.start_time

            # Filter by API type
            classification_records = [r for r in self.records if r.api_type == 'classification']
            extraction_records = [r for r in self.records if r.api_type == 'extraction']

            return {
                "api_requests": {
                    "classification_total": len(classification_records),
                    "extraction_total": len(extraction_records),
                    "total": len(self.records)
                },
                "request_rate": {
                    "classification_rps_mean": len(classification_records) / total_duration if total_duration > 0 else 0,
                    "classification_rps_peak": self._calculate_peak_rps(classification_records),
                    "classification_rpm_mean": (len(classification_records) / total_duration) * 60 if total_duration > 0 else 0,
                    "classification_rpm_peak": self._calculate_peak_rps(classification_records) * 60,
                    "extraction_rps_mean": len(extraction_records) / total_duration if total_duration > 0 else 0,
                    "extraction_rps_peak": self._calculate_peak_rps(extraction_records),
                    "extraction_rpm_mean": (len(extraction_records) / total_duration) * 60 if total_duration > 0 else 0,
                    "extraction_rpm_peak": self._calculate_peak_rps(extraction_records) * 60
                },
                "errors": {
                    "error_429_count": len(self.errors_429),
                    "error_429_first_timestamp": self.errors_429[0].timestamp if self.errors_429 else None,
                    "error_429_rate_when_occurred": self._get_rate_at_first_429(),
                    "other_api_errors": len([r for r in self.records if not r.success and r.error_type and "429" not in str(r.error_type)])
                },
                "latency": {
                    "classification_p50_ms": self._percentile([r.duration_ms for r in classification_records], 50),
                    "classification_p95_ms": self._percentile([r.duration_ms for r in classification_records], 95),
                    "classification_p99_ms": self._percentile([r.duration_ms for r in classification_records], 99),
                    "extraction_p50_ms": self._percentile([r.duration_ms for r in extraction_records], 50),
                    "extraction_p95_ms": self._percentile([r.duration_ms for r in extraction_records], 95),
                    "extraction_p99_ms": self._percentile([r.duration_ms for r in extraction_records], 99)
                },
                "concurrency": {
                    "max_concurrent_total": self._calculate_max_concurrent(),
                    "max_concurrent_classification": self._calculate_max_concurrent(classification_records),
                    "max_concurrent_extraction": self._calculate_max_concurrent(extraction_records)
                }
            }

    def _calculate_peak_rps(self, records: list[APICallRecord], window_seconds: float = 1.0) -> float:
        """Calculate peak requests per second using sliding window."""
        if not records:
            return 0.0

        sorted_records = sorted(records, key=lambda r: r.timestamp)
        max_rps = 0.0

        for i, record in enumerate(sorted_records):
            window_end = record.timestamp + window_seconds
            count = 1  # Include current record

            # Count records in window
            for j in range(i + 1, len(sorted_records)):
                if sorted_records[j].timestamp <= window_end:
                    count += 1
                else:
                    break

            rps = count / window_seconds
            max_rps = max(max_rps, rps)

        return max_rps

    def _percentile(self, values: list[float], p: int) -> float:
        """Calculate percentile."""
        if not values:
            return 0.0
        sorted_values = sorted(values)
        index = int((p / 100.0) * len(sorted_values))
        return sorted_values[min(index, len(sorted_values) - 1)]

    def _calculate_max_concurrent(self, records: list[APICallRecord] | None = None) -> int:
        """Calculate maximum concurrent requests."""
        if records is None:
            records = self.records

        if not records:
            return 0

        # Create events (start, end) for each request
        events = []
        for record in records:
            start = record.timestamp
            end = record.timestamp + (record.duration_ms / 1000.0)
            events.append((start, 1))   # Request start
            events.append((end, -1))     # Request end

        # Sort by timestamp
        events.sort()

        # Calculate max concurrent
        current = 0
        max_concurrent = 0
        for timestamp, delta in events:
            current += delta
            max_concurrent = max(max_concurrent, current)

        return max_concurrent

    def _get_rate_at_first_429(self) -> dict:
        """Get request rate when first 429 error occurred."""
        if not self.errors_429:
            return {}

        first_429 = self.errors_429[0]
        window_seconds = 1.0

        # Count requests in 1-second window before error
        window_start = first_429.timestamp - window_seconds

        classification_count = len([
            r for r in self.records
            if r.api_type == 'classification' and window_start <= r.timestamp <= first_429.timestamp
        ])

        extraction_count = len([
            r for r in self.records
            if r.api_type == 'extraction' and window_start <= r.timestamp <= first_429.timestamp
        ])

        return {
            "classification_rps": classification_count / window_seconds,
            "extraction_rps": extraction_count / window_seconds
        }

    def _empty_metrics(self) -> dict:
        """Return empty metrics structure."""
        return {
            "api_requests": {"classification_total": 0, "extraction_total": 0, "total": 0},
            "request_rate": {
                "classification_rps_mean": 0, "classification_rps_peak": 0,
                "classification_rpm_mean": 0, "classification_rpm_peak": 0,
                "extraction_rps_mean": 0, "extraction_rps_peak": 0,
                "extraction_rpm_mean": 0, "extraction_rpm_peak": 0
            },
            "errors": {
                "error_429_count": 0,
                "error_429_first_timestamp": None,
                "error_429_rate_when_occurred": {},
                "other_api_errors": 0
            },
            "latency": {
                "classification_p50_ms": 0, "classification_p95_ms": 0, "classification_p99_ms": 0,
                "extraction_p50_ms": 0, "extraction_p95_ms": 0, "extraction_p99_ms": 0
            },
            "concurrency": {
                "max_concurrent_total": 0,
                "max_concurrent_classification": 0,
                "max_concurrent_extraction": 0
            }
        }

    def reset(self):
        """Reset all tracking data."""
        with self._data_lock:
            self.records.clear()
            self.errors_429.clear()
            self.start_time = time.time()


# Global singleton instance
_tracker = APIMetricsTracker()


def get_tracker() -> APIMetricsTracker:
    """Get the global metrics tracker instance."""
    return _tracker
