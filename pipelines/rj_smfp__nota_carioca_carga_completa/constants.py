"""Constantes da carga completa da Nota Carioca."""

# ``<nome do flow>/<nome do deployment>`` dos deployments de prod encadeados por padrão (conferidos na API do Prefect).
# O deployment de staging da carga completa passa os de staging pelos parâmetros ``oracle_to_bq_deployment`` e
# ``bq_to_oracle_deployment`` (prefect.yaml).
ORACLE_TO_BQ_DEPLOYMENT = "rj-smfp--nota-carioca-oracle-to-bq/rj-smfp--nota-carioca-oracle-to-bq--prod"
BQ_TO_ORACLE_DEPLOYMENT = "rj-smfp--nota-carioca-bq-to-oracle/rj-smfp--nota-carioca-bq-to-oracle--prod"

# Carregador das exigíveis no Postgres (``incluir_postgres``); o de staging é passado pelo parâmetro
# ``bq_to_postgres_deployment`` (prefect.yaml).
BQ_TO_POSTGRES_DEPLOYMENT = "rj-smfp--nota-carioca-bq-to-postgres/rj-smfp--nota-carioca-bq-to-postgres--prod"

# Onde o ``bq_to_oracle`` grava o export e o manifesto (``gs://<bucket>/<dataset>/<flow_run_id>/manifesto.json``): os
# padrões dos parâmetros ``gcs_bucket`` e ``dataset_id`` do deployment de prod dele. O de staging usa o dataset
# ``nota_carioca_staging`` (``export_dataset_id`` no prefect.yaml).
EXPORT_BUCKET = "rj-iplanrio-dia-bq-to-oracle"
EXPORT_DATASET_ID = "nota_carioca"

ORACLE_TO_BQ_LABEL = "Oracle → BigQuery"
BQ_TO_ORACLE_LABEL = "BigQuery → Oracle"
BQ_TO_POSTGRES_LABEL = "BigQuery → Postgres"

SECONDS_PER_HOUR = 3600
# O pod só espera: consultar o estado do filho a cada 30 s basta (o padrão do Prefect é 5 s, por horas).
POLL_SECONDS = 30.0
