"""Constantes do extrator do ADN e do backfill do histórico."""

PROJETO_GCP = "rj-iplanrio-dia"
BUCKET_RAW = "rj-iplanrio-dia-nfse-nacional-raw"
DATASET_RAW = "raw_nfse_nacional"
TABELA_DOCUMENTOS = "documentos"
TABELA_CURSOR = "adn_cursor"
TABELA_INEXISTENTES = "adn_nsu_inexistente"
AMBIENTES = ("staging", "prod")
"""``staging`` consulta a produção restrita do ADN; ``prod``, a produção."""

ORIGEM_ADN = "adn"
ORIGEM_ORACLE_BACKFILL = "oracle_backfill"
EXTENSAO_LOTE = ".ndjson.gz"
PARTICAO_SEM_DATA = "1970-01-01"
"""Data do ``dt=`` de um lote em que nenhum documento tem ``DataHoraGeracao``."""

INTERVALO_ENTRE_CHAMADAS_S = 2.0
MAX_LACUNAS_POR_RUN = 10_000
LACUNAS_POR_OBJETO = 50
VALIDADE_TRAVA_S = 1800.0
