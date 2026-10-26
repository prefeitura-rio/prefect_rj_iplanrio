"""The notices that the dispatch posts to Discord."""

from collections.abc import Mapping
from json import dumps

from pipelines.rj_crm__disparo_template.constants import SUCCESS_SHARE
from pipelines.rj_crm__disparo_template.types import Report
from utils.dispatch import MENTIONS

CPF_VISIBLE = 3


def mask_cpf(cpf: object) -> str:
    """Show the first three characters of a CPF and hide the rest."""
    text = str(cpf or "")

    return text[:CPF_VISIBLE] + "*" * max(0, len(text) - CPF_VISIBLE)


def sample_text(row: Mapping[str, object]) -> str:
    """Return a row as an example in JSON: phone, masked CPF and the other columns."""
    extra = {key: value for key, value in row.items() if key not in {"telefone", "cpf"}}
    sample = {"telefone": row.get("telefone", ""), "cpf": mask_cpf(row.get("cpf")), **extra}

    return dumps({key: str(value) for key, value in sample.items()}, indent=2, ensure_ascii=False)


def success_notice(report: Report, sample: Mapping[str, object] | None, attempt: int, attempts: int) -> str:
    """Return the notice of a dispatch that went out, with a masked example of a row."""
    title = (
        "✅ **[TESTE] Disparo Realizado com Sucesso**" if report.test_mode else "✅ **Disparo Realizado com Sucesso**"
    )
    text = (
        f"{title}\n📊 **Quantidade:** {report.total} disparos\n📦 **Lotes:** {report.batches} lotes\n"
        f"🕐 **Hora:** {report.dispatch_date}\n📋 **Campanha:** {report.campaign}\n"
        f"*️⃣ **Tentativa:** {attempt} de {attempts}\n"
    )

    if sample:
        text += f"\n📱 **Exemplo de Disparo:**\n```json\n{sample_text(sample)}\n```\n"

    return text


def missing_status_notice(campaign: str, minutes: int) -> str:
    """Return the notice of a dispatch with no status after the time that is watched."""
    return (
        f"{MENTIONS}\n🚨 **Nenhum webhook de sucesso recebido!**\n📋 **Campanha:** {campaign}\n"
        f"⏱️ **Prazo monitorado:** {minutes} minutos após o envio via SFTP.\n"
    )


def unknown_campaign_notice(campaign: str) -> str:
    """Return the notice of a campaign that is not in the table of journeys.

    Its status is lost.
    """
    return (
        f"<@821121576455634955> <@302518123066556426>\nATENÇÃO: campaign_name='{campaign}' não encontrado na coluna "
        "hsm.nome_hsm da tabela rj-crm-registry.brutos_salesforce.jornada. Devido a isso os dados não "
        "aparecerão na tabela `status_disparo`."
    )


def is_confirmed(count: int, total: int) -> bool:
    """Tell whether enough sends have a status for the campaign to count as sent.

    It takes at least a fifth of them.
    """
    return count >= total * SUCCESS_SHARE
