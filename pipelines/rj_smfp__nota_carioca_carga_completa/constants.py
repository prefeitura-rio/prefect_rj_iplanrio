"""Constantes da carga completa da Nota Carioca."""

# ``<nome do flow>/<nome do deployment>`` dos deployments de prod encadeados por padrão (conferidos na API do Prefect).
# O deployment de staging da carga completa passa os de staging pelos parâmetros ``oracle_to_bq_deployment`` e
# ``bq_to_oracle_deployment`` (prefect.yaml).
ORACLE_TO_BQ_DEPLOYMENT = "rj-smfp--nota-carioca-oracle-to-bq/rj-smfp--nota-carioca-oracle-to-bq--prod"
BQ_TO_ORACLE_DEPLOYMENT = "rj-smfp--nota-carioca-bq-to-oracle/rj-smfp--nota-carioca-bq-to-oracle--prod"
# Extração XML (documentos fiscais nacionais → DFEN) na aplicação, que antes era acionada à mão: roda primeiro.
EXTRACTION_DEPLOYMENT = "rj-smfp--nota-carioca-extracao-xml/rj-smfp--nota-carioca-extracao-xml--prod"

EXTRACTION_LABEL = "Extração XML"
ORACLE_TO_BQ_LABEL = "Oracle → BigQuery"
BQ_TO_ORACLE_LABEL = "BigQuery → Oracle"

SECONDS_PER_HOUR = 3600
# O pod só espera: consultar o estado do filho a cada 30 s basta (o padrão do Prefect é 5 s, por horas).
POLL_SECONDS = 30.0
