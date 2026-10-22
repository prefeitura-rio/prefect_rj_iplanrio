"""Constants of rj_crm__wetalkie_api_hsm_info."""

from typing import Final
from zoneinfo import ZoneInfo

BASE_URL: Final = "https://api.whatsapp.dados.rio/v1"
LOGIN_URL: Final = f"{BASE_URL}/users/login"
TEMPLATES_URL: Final = f"{BASE_URL}/callcenter/hsms"
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
PAGE_SIZE: Final = 100
MAX_PAGES: Final = 100
"""The most pages of one list, so that a broken `hasNextPage` never loops for ever."""
FIELDS: Final = (
    "id,name,message,metaMessageTemplateCategory,qualityScore,modelRejection,"
    "metaMessageTemplateStatus,metaMessageTemplateId,templateId,whatsappConfig"
)
CODED_FIELDS: Final = ("modelRejection", "metaMessageTemplateStatus", "metaMessageTemplateCategory", "qualityScore")
"""The fields that are an object with a code and a description."""
