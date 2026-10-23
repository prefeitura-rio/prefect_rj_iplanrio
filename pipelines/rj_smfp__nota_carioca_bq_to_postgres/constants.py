"""Constantes do carregador das exigíveis no Postgres."""

PG_DSN_ENV = "PG_NOTA_CARIOCA__DSN"
"""Variável de ambiente com o DSN do Postgres do carregador (login em ``nc_carregador``), vinda do segredo do pod."""

DEFAULT_PROJECT = "rj-iplanrio-dia"
"""Projeto do GCS onde o ``bq_to_oracle`` deixa o export."""

SOURCE_PREFIX = "bq_to_oracle"
"""Prefixo de ``publicacoes.fonte``: ``bq_to_oracle:<dataset>``."""

FLOW_LABEL = "BigQuery → Postgres"
"""Nome do fluxo nas mensagens do Discord."""
