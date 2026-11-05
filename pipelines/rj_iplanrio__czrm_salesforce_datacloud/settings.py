"""Environment variables read by this pipeline via pydantic-settings.

Secrets are injected by `utils.secrets` from Infisical before the flow starts.
The Infisical path `/rj_iplanrio__czrm_salesforce_datacloud` holds variables
named `SF_DC_<ORG>_CLIENT_ID`, `SF_DC_<ORG>_CLIENT_SECRET`, etc.

`SalesforceSettings` is instantiated with a dynamic prefix to select the right
org: `SalesforceSettings(_env_prefix="SF_DC_PROD_")`.  `ToolkitSettings` sets
`extra="ignore"` so the other orgs' variables are silently skipped.

`DiscordSettings` is re-exported from `toolkit.settings` for convenience —
the webhook URL lives in the `/shared` Infisical path.
"""

from pydantic import SecretStr
from toolkit.settings import DiscordSettings, ToolkitSettings, required_secret, required_text

__all__ = ["DiscordSettings", "SalesforceSettings"]


class SalesforceSettings(ToolkitSettings):
    """OAuth2 Client Credentials for one Salesforce org.

    Instantiate with `_env_prefix` matching the origin:

        SalesforceSettings(_env_prefix="SF_DC_PROD_")
        SalesforceSettings(_env_prefix="SF_DC_STAGING_")
        SalesforceSettings(_env_prefix="SF_DC_QA_")

    The Infisical sync exposes variables as `SF_DC_<ORG>_CLIENT_ID`, etc.
    `ToolkitSettings` ignores extra keys, so the other orgs are skipped.
    """

    CLIENT_ID: str = required_text()
    CLIENT_SECRET: SecretStr = required_secret()
    INSTANCE_URL: str = required_text()
    DATASPACE: str = required_text()
