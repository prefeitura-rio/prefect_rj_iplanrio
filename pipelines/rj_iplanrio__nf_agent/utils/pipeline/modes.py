"""Pipeline execution modes for granular control over processing steps."""

from enum import Enum


class ExecutionMode(Enum):
    """Pipeline execution modes for granular control over processing steps."""

    FULL = "full"  # Complete pipeline (default)
    PREPROCESS_CLASSIFICATION = "preprocess_classification"  # Step 1: Generate classification inputs
    RUN_CLASSIFICATION = "run_classification"  # Step 2: Run classification API
    PREPROCESS_EXTRACTION = "preprocess_extraction"  # Step 3: Generate extraction inputs
    RUN_EXTRACTION = "run_extraction"  # Step 4: Run extraction API
    VALIDATE = "validate"  # Step 5: Run validation only
