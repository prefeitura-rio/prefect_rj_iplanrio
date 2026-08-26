"""Default Gemini service account path, shared by ``classification`` and ``extraction``.

Both packages fall back to Application Default Credentials (ADC) when this
file doesn't exist, which is the case in production (credentials are
injected via ADC/GCP service account, not a local file).
"""

from pathlib import Path

SERVICE_ACCOUNT_PATH = Path(__file__).parent / "credentials" / "gemini-service-account.json"
