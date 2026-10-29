"""Constantes do carregador das exigíveis no Postgres."""

PG_DSN_ENV = "PG_NOTA_CARIOCA__DSN"
"""Variável de ambiente com o DSN do Postgres do carregador (login em ``nc_carregador``), vinda do segredo do pod."""

DEFAULT_PROJECT = "rj-iplanrio-dia"
"""Projeto do GCS onde o ``bq_to_oracle`` deixa o export."""

SOURCE_PREFIX = "bq_to_oracle"
"""Prefixo de ``publicacoes.fonte``: ``bq_to_oracle:<dataset>``."""

FLOW_LABEL = "BigQuery → Postgres"
"""Nome do fluxo nas mensagens do Discord."""

SYNC_PG_DSN_ENV = "PG_NOTA_CARIOCA_SINCRONIZADOR__DSN"
"""DSN do Postgres do sincronizador de contribuintes: um login em ``nc_sincronizador``, que só escreve em
``mv_contribuintes`` e na marca d'água; o login do carregador (``nc_carregador``) não escreve ali."""

ORACLE_LEITURA_SECRET_PATH = "/db-oracle-nc-leitura"
"""Pasta do Infisical do usuário somente leitura do Oracle: ``DB_ORACLE_NC_LEITURA__DB_HOST`` e as outras quatro."""

CONTRIBUINTES_ORACLE_TABLE = "NFSE_RPE.MV_CONTRIBUINTES"
"""Tabela (view materializada) de contribuintes no Oracle, como as consultas de produção a leem."""
