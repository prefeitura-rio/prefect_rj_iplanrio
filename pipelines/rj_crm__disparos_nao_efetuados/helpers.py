"""Helpers of rj_crm__disparos_nao_efetuados."""

from collections.abc import Mapping, Sequence
from datetime import datetime

from pipelines.rj_crm__disparos_nao_efetuados.constants import FAILURE_MENTIONS, NOTICE_MENTIONS, SP_TZ
from utils.utils import render_query


def build_query() -> str:
    """Return the query of the active campaigns that sent nothing today."""
    return render_query(__file__, "campanhas_sem_disparo")


def format_today() -> str:
    """Return today in São Paulo as `DD/MM/YYYY`."""
    return datetime.now(SP_TZ).strftime("%d/%m/%Y")


def build_missing_message(campaigns: Sequence[Mapping[str, object]], today: str) -> str:
    """Return the notice that lists the active campaigns with no dispatch today."""
    lines = "\n".join(f"• `{campaign['nome_campanha']} - {campaign['nome_hsm']}`" for campaign in campaigns)

    return (
        f"{NOTICE_MENTIONS}\n"
        f"⚠️ **Campanhas ativas SEM disparo hoje ({today}):**\n\n"
        f"{lines}\n\n"
        f"Total: **{len(campaigns)}** campanha(s) sem disparo."
    )


def build_complete_message(today: str) -> str:
    """Return the notice that every active campaign sent something today."""
    return f"{NOTICE_MENTIONS}\n✅ **Todas as campanhas ativas tiveram disparo hoje ({today}).**"


def build_failure_message(flow_name: str, flow_run_name: str, state_name: str) -> str:
    """Return the notice of a failed run."""
    return (
        f"{FAILURE_MENTIONS}\n"
        "Prefect flow de verificação de disparos não efetuados falhou! 🚨\n"
        f"Flow: {flow_name}\n"
        f"Flow Run: {flow_run_name}\n"
        f"State: {state_name}"
    )
