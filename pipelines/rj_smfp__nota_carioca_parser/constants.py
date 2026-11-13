"""Constantes do parser de NFS-e Nacional."""

from pathlib import Path

PIPELINE_DIR = Path(__file__).parent
SCHEMAS_DIR = PIPELINE_DIR / "schemas"
SCHEMAS_BQ_DIR = PIPELINE_DIR / "schemas_bq"
# load_query resolve queries/ ao lado do arquivo recebido; esta constante aponta para a raiz da pipeline.
QUERIES_ANCHOR = __file__

# Identifica o conjunto de regras (sanitização, XSD, tolerâncias, mapeamento). Mudou o parser, mudou a versão: a marca
# começa do zero para a versão nova e a quarentena é reavaliada (devolução). Nunca reutilize um valor já publicado.
VERSAO_PARSER = "2026.10.1"

PROJECT_ID = "rj-iplanrio-dia"
BUCKET_RAW = "rj-iplanrio-dia-nfse-nacional-raw"
DATASET_RAW = "raw_nfse_nacional"
DATASET_RAW_STAGING = "raw_nfse_nacional_staging"
AMBIENTE_PROD = "prod"
AMBIENTE_STAGING = "staging"

TABELA_INDICE = "documentos"
TABELA_MARCA = "parser_marca"
TABELA_QUARENTENA = "quarentena"
TABELAS_RAW = ("nfse", "dps", "pessoas", "eventos")

# Quantidade de NSUs lidos do índice por lote; a marca avança a cada lote carregado por inteiro.
NSUS_POR_LOTE = 5_000
# Piso do quarentena reavaliada por execução (devolução), em documentos; o excedente fica para a próxima execução.
QUARENTENA_POR_EXECUCAO = 5_000
# Teto de rodadas de revalidação depois que nós tolerados são removidos (o libxml2 para no primeiro erro estrutural).
MAX_RODADAS_VALIDACAO = 10

FUSO_ORACLE = "America/Sao_Paulo"
IBGE_RIO_DE_JANEIRO = "3304557"
