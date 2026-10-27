"""Constants of rj_crm__disparos_nao_efetuados."""

from typing import Final
from zoneinfo import ZoneInfo

BILLING_PROJECT: Final = "rj-crm-registry"
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
FAILURE_MENTIONS: Final = "<@821121576455634955> <@1458456241683824744> <@302518123066556426>"
NOTICE_MENTIONS: Final = f"{FAILURE_MENTIONS} <@1524151359765479475>"
UNKNOWN_URL: Final = "URL indisponível"
