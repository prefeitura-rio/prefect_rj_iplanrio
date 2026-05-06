"""
Prefect entrypoint for the NF (Nota Fiscal) validation pipeline.

agent-nf-validator is cloned to /opt/agent-nf-validator at image build time
and exposed via PYTHONPATH. run_poc uses bare imports (e.g. `from database
import DatabaseManager`), so we also add its own directory to sys.path.
"""

import sys

sys.path.insert(0, "/opt/agent-nf-validator/run_poc")

from run_poc.run_pipeline import nf_processing_flow  # noqa: F401, E402

__all__ = ["nf_processing_flow"]
