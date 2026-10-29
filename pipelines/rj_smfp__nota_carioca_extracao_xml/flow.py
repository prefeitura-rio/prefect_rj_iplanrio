"""Flow for rj_smfp__nota_carioca_extracao_xml.

Dispara a extração dos documentos fiscais nacionais (XML → DFEN) da aplicação Nota Carioca, que antes era acionada à
mão, e só termina com sucesso quando ela e o pós-processamento acabam bem: chama a operação SOAP
``ExtrairDocumentosFiscaisNacionaisAutomatico`` (token e ``mode=1``), acompanha a tarefa devolvida em
``NFSE_OWNER.TAREFAS`` (``DATA_TERMINO`` preenchida e ``STATUS = 2``) e espera não haver extração (46) nem
pós-processamento (48) em execução, a condição da guarda do ``oracle_to_bq``. ``Conflict`` (extração já em execução)
acompanha a extração em curso; ``OK`` sem tarefa (nada pendente) segue direto. Qualquer outro desfecho falha o flow, o
que impede a carga completa de seguir.

O ambiente vem do nome do deployment: staging chama a homologação (``notacariocahom``) e lê o Oracle de homologação;
prod chama a produção. Token (variável ``NOTA_CARIOCA_ROTINAS_TOKEN``) e Oracle (``DB_ORACLE_NOTA_FISCAL__*``) vêm do
Infisical do Prefect. O flow nunca escreve no Oracle e nunca interrompe a tarefa da aplicação, nem no tempo limite.
"""

from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import (
    INFISICAL_SECRET_PATH,
    MANUAL_MODE,
    SECONDS_PER_HOUR,
    SECONDS_PER_MINUTE,
)
from pipelines.rj_smfp__nota_carioca_extracao_xml.tasks import run_extraction_task
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.config import resolve_url
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.discord import current_environment
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.wait import Limits


@flow(log_prints=True)
def rj_smfp__nota_carioca_extracao_xml(  # noqa: PLR0913, PLR0917
    url: str | None = None,
    mode: int = MANUAL_MODE,
    infisical_secret_path: str = INFISICAL_SECRET_PATH,
    poll_seconds: int = 60,
    timeout_hours: int = 8,
    stale_hours: int = 6,
    idle_checks: int = 2,
    progress_minutes: int = 30,
    discord_notifications: bool = True,
) -> str:
    """Dispara a extração XML e espera ela e o pós-processamento terminarem.

    :param url: Endereço do ``RotinasAutomaticas.asmx``; vazio usa o do ambiente (staging = homologação).
    :param mode: ``mode`` do webservice (0 automático, 1 manual).
    :param infisical_secret_path: Pasta do Infisical com o Oracle.
    :param poll_seconds: Intervalo entre consultas a ``NFSE_OWNER.TAREFAS``.
    :param timeout_hours: Tempo máximo, da chamada até a liberação; no limite o flow falha e a tarefa segue no Oracle.
    :param stale_hours: Horas em execução a partir das quais uma tarefa 46/48 é tratada como travada (como na guarda).
    :param idle_checks: Consultas seguidas sem 46/48 em execução para considerar liberado.
    :param progress_minutes: Intervalo entre avisos de progresso no Discord.
    :param discord_notifications: ``False`` desliga o Discord.
    :returns: Resumo do que aconteceu.
    """
    rename_current_flow_run_task(new_name="extracao-xml")
    environment = current_environment()
    limits = Limits(
        poll_seconds=poll_seconds,
        deadline_seconds=timeout_hours * SECONDS_PER_HOUR,
        stale_hours=stale_hours,
        idle_checks=idle_checks,
        progress_every_seconds=progress_minutes * SECONDS_PER_MINUTE,
    )
    return run_extraction_task(
        url=resolve_url(url, environment),
        mode=mode,
        infisical_secret_path=infisical_secret_path,
        limits=limits,
        discord_notifications=discord_notifications,
        environment=environment,
    )
