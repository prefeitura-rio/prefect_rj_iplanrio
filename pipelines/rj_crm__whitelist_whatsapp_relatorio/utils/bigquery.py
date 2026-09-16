"""Leitura do BigQuery.

A credencial vem de ``get_bd_credentials_from_env``, API pública do ``iplanrio`` (D42).
Mesmo caminho de `rj_crm__get_history_data`, `rj_crm__agentforce_classificacao_llm` e
`rj_crm__salesforce_agentforce_api`, que leem este mesmo projeto.

Credencial explícita não é redundância: o docstring de
`rj_crm__agentforce_classificacao_llm/utils/bigquery.py` registra 403 ao confiar no ADC
dentro do pod do work pool, lendo `brutos_salesforce` — a mesma fonte deste relatório.
"""

from time import sleep

import pandas as pd
from google.auth.credentials import Credentials
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env, getenv_or_action

from pipelines.rj_crm__whitelist_whatsapp_relatorio.utils.log import logger_da_pipeline

logger = logger_da_pipeline(__name__)

BILLING_PROJECT_ID = "rj-crm-registry"
"""Projeto cobrado pela consulta.

Hard coded por decisão (D28): é o padrão do repo, não é segredo e muda junto com o
código. Só este módulo precisa dele, então mora aqui e não em ``constants.py`` (§4.5).
"""

MODE_CREDENCIAIS = "prod"
"""Modo da service account, fixo em ``prod`` mesmo no deployment de staging (D39).

Não é descuido: 59 dos 64 call sites do repositório fazem o mesmo, e a razão está
documentada em `rj_crm__agentforce_classificacao_llm/utils/bigquery.py` — cada secret
(``prefect-jobs-crm-registry-secrets`` e ``-staging``) já define
``BASEDOSDADOS_CREDENTIALS_PROD`` com a conta certa do seu próprio ambiente. Derivar o
modo do ``environment`` leria ``..._STAGING``, que guarda outra coisa.
"""

VARIAVEIS_CREDENCIAIS = (
    "BASEDOSDADOS_CREDENTIALS_PROD",
    "BASEDOSDADOS_CREDENTIALS_STAGING",
    "BASEDOSDADOS_CONFIG",
)
"""Variáveis exigidas pelo ``iplanrio`` para ler ou injetar credenciais.

Tanto ``get_bd_credentials_from_env`` quanto ``inject_bd_credentials`` chamam
``validate_bd_credentials`` antes de qualquer coisa, e essa função levanta se **qualquer
uma** das três faltar — inclusive a do ambiente que não está em uso. Conferir só a que
vamos ler deixaria a exceção estourar no meio do flow.
"""


def credenciais_ausentes() -> list[str]:
    """Aponta o que falta para usar a service account do ambiente.

    ``action="ignore"`` porque aqui a ausência é resposta, não erro: quem decide o que
    fazer com ela é o chamador, e em execução local ela é o caminho esperado (D35).

    :returns: Nomes das variáveis ausentes; vazio quando a service account está completa.
    """
    return [nome for nome in VARIAVEIS_CREDENCIAIS if not getenv_or_action(nome, action="ignore")]


def resolver_credenciais() -> Credentials | None:
    """Escolhe a credencial de acesso ao BigQuery.

    Em staging e produção a service account chega pelo Kubernetes Secret e é ela que
    vale. Faltando qualquer variável — caso do ambiente local — devolve ``None``, e o
    cliente cai nas credenciais padrão do ambiente (ADC do ``gcloud``), conforme a D35.

    :returns: Credencial da service account, ou ``None`` para usar o padrão do ambiente.
    """
    ausentes = credenciais_ausentes()
    if ausentes:
        logger.warning(
            "%s ausente(s): usando as credenciais padrão do ambiente (ADC). "
            "Isso é esperado em execução local e não valida as permissões da service account.",
            ", ".join(ausentes),
        )
        return None

    logger.info("Usando a service account de BASEDOSDADOS_CREDENTIALS_%s", MODE_CREDENCIAIS.upper())
    return get_bd_credentials_from_env(mode=MODE_CREDENCIAIS)


def baixar(query: str) -> pd.DataFrame:
    """Executa uma query no BigQuery e devolve o resultado como DataFrame.

    :param query: SQL já renderizado.
    :returns: Resultado da query.
    :raises google.api_core.exceptions.GoogleAPIError: Se a execução falhar no BigQuery.
    """
    logger.info("Consultando o BigQuery no projeto %s", BILLING_PROJECT_ID)
    cliente = bigquery.Client(credentials=resolver_credenciais(), project=BILLING_PROJECT_ID)
    job = cliente.query(query)
    while not job.done():
        sleep(1)
    dados = job.result().to_dataframe(create_bqstorage_client=False)
    logger.info("Query retornou %d linha(s)", len(dados))
    return dados
