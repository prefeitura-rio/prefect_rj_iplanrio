"""
Helper to inject GCP service account credentials from base64-encoded environment variables.

Usage (encode credentials for Infisical):
    cat service-account.json | base64

Then store the output as GCP_CREDENTIALS_BASE64 in Infisical.
At runtime, call inject_credentials_from_env() once at startup — all GCP clients
(GCS, Gemini, BigQuery) then pick up credentials automatically via ADC.
"""

import base64
import os


def inject_credentials_from_env(env_var: str = "GCP_CREDENTIALS_BASE64") -> bool:
    """
    Decode a base64-encoded service account from an env var and inject it as ADC.

    Writes the decoded JSON to /tmp/nf_agent_credentials.json and sets
    GOOGLE_APPLICATION_CREDENTIALS so all GCP SDKs pick it up automatically.

    :param env_var: Name of the environment variable (default: GCP_CREDENTIALS_BASE64).
    :returns: True if credentials were injected, False if env var is not set.
    :raises ValueError: If the env var is set but contains invalid base64.
    """
    value = os.getenv(env_var)
    if not value:
        return False

    try:
        decoded = base64.b64decode(value)
    except Exception as e:
        raise ValueError(
            f"Failed to decode credentials from env var '{env_var}'. "
            f"Make sure the value is a valid base64-encoded service account JSON.\n"
            f"To encode: cat service-account.json | base64\n"
            f"Error: {e}"
        )

    creds_path = "/tmp/nf_agent_credentials.json"
    with open(creds_path, "wb") as f:
        f.write(decoded)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
    print(f"[INFO] Credentials injected from {env_var} → {creds_path}")
    return True
