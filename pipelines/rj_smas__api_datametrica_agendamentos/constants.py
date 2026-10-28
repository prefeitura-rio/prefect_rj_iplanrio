"""Constants of rj_smas__api_datametrica_agendamentos."""

from typing import Final
from zoneinfo import ZoneInfo

SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
WEEKEND: Final = (5, 6)
"""The weekdays (Monday is 0) on which the flow does nothing."""
LONG_AHEAD: Final = (3, 4)
"""Thursday and Friday look four days ahead, to cover the weekend.

Other days look two.
"""
DISPATCH_DEPLOYMENT: Final = "rj-crm--disparo-template-sf/rj-crm--disparo-template-sf--prod"
USER_AGENT: Final = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
NO_APPOINTMENT: Final = "Nenhum agendamento encontrado"
"""The start of the message that the API sends for a date with no appointment."""
