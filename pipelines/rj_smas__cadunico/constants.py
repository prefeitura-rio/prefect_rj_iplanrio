"""Constants of rj_smas__cadunico."""

from typing import Final

from pipelines.rj_smas__cadunico.types import RawFolders, Tables

DEFAULT_RAW: Final[RawFolders] = {
    "bucket": "rj-smas",
    "records_prefix": "raw/protecao_social_cadunico/registro_familia",
    "layout_prefix": "raw/protecao_social_cadunico/layout",
}
DEFAULT_TABLES: Final[Tables] = {"dataset_id": "brutos_cadunico", "records": "registro_familia", "layout": "layout"}
MAX_CONCURRENT: Final = 3
"""The ZIP files of records that load at one time."""
COMMIT_AUTHOR: Final = ("CadUnico Pipeline", "pipeline@prefeitura.rio")
MODELS_FOLDER: Final = "models/raw/smas/protecao_social_cadunico"
MODEL_PREFIX: Final = "raw_protecao_social_cadunico"
WORK_FOLDER: Final = "cadunico_"
DELIMITER: Final = "\x1f"
"""The text lines of CadÚnico have no such character, so one line is one column."""
BLOCK_SIZE: Final = 1 << 26
BATCHES_PER_FILE: Final = 16
"""The batches of one Parquet file: about 1 GiB of text."""
VERSION_IN_HEADER: Final = slice(69, 74)
DATE_IN_HEADER: Final = slice(82, 90)
HEADER_DATE_FORMAT: Final = "%d%m%Y"
TWO_DIGIT_YEAR_LENGTH: Final = 7
"""The length of `A` and a date `YYMMDD`."""
FULL_DATE_LENGTH: Final = 8
MONTH_DATE_LENGTH: Final = 6
LAYOUT_TITLE: Final = "LEIAUTE VERSÃO"
BLOB_VERSION_MARK: Final = "BMM_V"
LAYOUT_EXTENSIONS: Final = (".xlsx", ".xls")
TABLE_FIELDS: Final = (
    "reg",
    "campo",
    "arquivo_base_versao_7",
    "posicao",
    "tamanho",
    "tipo",
    "transformacao",
    "descricao",
    "nulos",
    "observacoes",
)
"""The columns of a table of the layout sheets, from the left."""
VERSION_DIGITS: Final = 4
"""The digits of a version in a file name, such as `0701` for the version 7.01."""
SKIPPED_TABLE: Final = "REG_99_DIARIA"
UNNAMED_COLUMN: Final = "sem_nome"
VERSION_CONTROL_SUFFIX: Final = "_columns_version_control"
DICTIONARY_URL: Final = "https://docs.google.com/spreadsheets/d/1VgtSVom_s4QsJoOws6caPZxGfwYrpjpoSjbOrqhRbM8/edit?gid=542906575#gid=542906575"
LAYOUT_FOLDER_URL: Final = (
    "https://console.cloud.google.com/storage/browser/rj-smas/raw/protecao_social_cadunico/layout"
)
MAX_DESCRIPTION: Final = 1024
"""BigQuery limits a column description to this many characters."""
DESCRIPTION_LIMIT: Final = 1020
EMPTY_COLUMN_MARK: Final = "vazio"
NULL_TEXTS: Final = frozenset({"nan", "none", "null", ""})
TABLE_NAMES: Final = {
    "00": "controle",
    "01": "identificacao_controle",
    "02": "domicilio",
    "03": "familia",
    "04": "identificacao_primeira_pessoa",
    "05": "documento_pessoa",
    "06": "pessoa_deficiencia",
    "07": "escolaridade",
    "08": "trabalho_remuneracao",
    "09": "contato",
    "11": "seguranca_alimentar",
    "12": "condicao_rua",
    "13": "status_familia",
    "14": "documento_membro",
    "15": "habitacao",
    "16": "transferencia_familia",
    "17": "identificacao_membro",
    "18": "exclusao_servidor",
    "19": "exclusao_membro",
    "20": "representante_legal",
    "21": "renda",
    "98": "prefeitura",
    "99": "registros",
}
"""The name of the table of each type of record, by its two digits."""
