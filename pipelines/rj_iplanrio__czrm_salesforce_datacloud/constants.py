"""Constantes da pipeline CZRM (Salesforce) Data Cloud -> BigQuery.

Só o que é usado por dois ou mais módulos, ou que precisa ser o mesmo em
todos eles (nome do dataset, prefixo das variáveis de ambiente). O resto
mora no módulo que usa.
"""

PROJECT_ID = "rj-iplanrio"
"""Projeto GCP de destino. Sobrescrito pelo parâmetro ``project_id``."""

DATASET_ID = "brutos_czrm_salesforce_staging"
"""Zona de pouso (pré-dbt) no padrão ``brutos_<assunto>_staging``.

O dbt escreve a camada curada em ``brutos_czrm_salesforce`` (fora desta
pipeline). O sufixo ``_staging`` aqui significa "antes do dbt", não
"ambiente de staging" — a mesma convenção usada por todas as áreas.
"""

DATASPACE = "default"
"""Dataspace do Data Cloud onde as DLOs vivem nas três orgs."""

API_VERSION = "v67.0"
"""Versão da API REST do Salesforce."""

PAGE_SIZE = 1000
"""Linhas por página. O teto real do servidor varia entre ~1400 e ~1700 por
resposta (limite de payload, não de contagem); 1000 mantém folga."""

MAX_ROWS = 5_000_000
"""Trava de segurança contra paginação infinita."""

TIMEOUT = 120
"""Timeout (segundos) por requisição HTTP."""

ENV_PREFIX = "CZRM_SALESFORCE"
"""Prefixo das variáveis injetadas pelo Infisical (pasta ``czrm-salesforce``)
no secret do work pool. O sync gera ``<PASTA_MAIUSCULA>__<CHAVE>``."""

DISCORD_WEBHOOK_ENV = "DISCORD_WEBHOOK_URL_CZRM_SALESFORCE"
"""Webhook opcional de resumo/falha. Ausente = notificação pulada."""
