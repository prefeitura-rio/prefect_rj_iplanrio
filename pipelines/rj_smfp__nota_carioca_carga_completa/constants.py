"""Constantes da carga completa da Nota Carioca."""

# ``<nome do flow>/<nome do deployment>`` dos deployments de prod encadeados (conferidos na API do Prefect).
ORACLE_TO_BQ_DEPLOYMENT = "rj-smfp--nota-carioca-oracle-to-bq/rj-smfp--nota-carioca-oracle-to-bq--prod"
BQ_TO_ORACLE_DEPLOYMENT = "rj-smfp--nota-carioca-bq-to-oracle/rj-smfp--nota-carioca-bq-to-oracle--prod"

ORACLE_TO_BQ_LABEL = "Oracle → BigQuery"
BQ_TO_ORACLE_LABEL = "BigQuery → Oracle"

SECONDS_PER_HOUR = 3600
# O pod só espera: consultar o estado do filho a cada 30 s basta (o padrão do Prefect é 5 s, por horas).
POLL_SECONDS = 30.0
