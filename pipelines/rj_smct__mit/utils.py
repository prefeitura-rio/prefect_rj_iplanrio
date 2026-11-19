"""Funções puras da pipeline rj_smct__mit."""

import json
import re
import tempfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import polars as pl
from google.cloud import storage

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

DOWNLOAD_WORKERS = 16

# Casa nomes como `OM_1_20251201_evening_12durmin`.
CAPTURE_NAME_PATTERN = re.compile(
    r"^(?P<route_id>OM_\d+)_(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})"
    r"_(?P<capture_period>[a-z]+)_(?P<capture_duration_min>\d+)durmin$"
)


@dataclass(frozen=True)
class TableConfig:
    """Configuração de origem dos arquivos de uma tabela.

    :param path: Pasta (terminada em ``/``) ou arquivo único, relativo ao prefixo da origem.
    :param file_format: Extensão dos arquivos de origem (``csv`` ou ``json``).
    :param parse_capture_name: Se verdadeiro, adiciona colunas extraídas do nome do arquivo
        (rota, data, período e duração da captura).
    """

    path: str
    file_format: str
    parse_capture_name: bool = False


# Tabelas pequenas primeiro: falhas de credencial ou de bucket aparecem antes do processamento pesado.
TABLES: dict[str, TableConfig] = {
    "calibration_equations": TableConfig("calibration/calibration_equations.csv", "csv"),
    "cross_validation_metrics": TableConfig("calibration/cross_validation_metrics.csv", "csv"),
    "data_dictionary": TableConfig("data_dictionary.csv", "csv"),
    "route_metadata": TableConfig("route_metadata/", "json"),
    "edge_analysis": TableConfig("edge_analysis/", "csv", parse_capture_name=True),
    "matched": TableConfig("matched/", "csv", parse_capture_name=True),
}


def get_table_config(table_id: str) -> TableConfig:
    """Retorna a configuração de uma tabela.

    :param table_id: Identificador da tabela. Deve ser uma chave de ``TABLES``.
    :returns: Configuração de origem da tabela.
    :raises ValueError: Se ``table_id`` não for suportado.
    """
    if table_id not in TABLES:
        raise ValueError(f"table_id não suportado: {table_id}. Esperado um de {list(TABLES)}")
    return TABLES[table_id]


def clean_column_name(name: str) -> str:
    """Normaliza um nome de coluna para um identificador snake_case aceito pelo BigQuery.

    :param name: Nome original da coluna.
    :returns: Nome em minúsculas, sem caracteres especiais e sem começar por dígito.
    """
    cleaned = re.sub(r"[^0-9a-zA-Z_]+", "_", name.strip()).strip("_").lower()
    if not cleaned or cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned


def parse_capture_name(file_stem: str) -> dict[str, str | None]:
    """Extrai rota, data, período e duração do nome de um arquivo de captura.

    :param file_stem: Nome do arquivo sem extensão.
    :returns: Dicionário com os valores extraídos, ou todos ``None`` se o nome não seguir o padrão.
    """
    match = CAPTURE_NAME_PATTERN.match(file_stem)
    if not match:
        logger.warning("Nome '%s' não segue o padrão de captura; colunas de captura ficarão nulas", file_stem)
        return {"route_id": None, "capture_date": None, "capture_period": None, "capture_duration_min": None}
    groups = match.groupdict()
    return {
        "route_id": groups["route_id"],
        "capture_date": f"{groups['year']}-{groups['month']}-{groups['day']}",
        "capture_period": groups["capture_period"],
        "capture_duration_min": groups["capture_duration_min"],
    }


def read_csv_as_strings(file_path: Path) -> pl.DataFrame:
    """Lê um CSV com todas as colunas como texto.

    Muitas colunas são vazias em alguns arquivos e preenchidas em outros, então inferir tipos por
    arquivo geraria schemas de Parquet diferentes para a mesma tabela. O cast fica para os modelos
    downstream.

    :param file_path: Caminho do arquivo CSV.
    :returns: DataFrame com colunas de texto e nomes normalizados.
    """
    dataframe = pl.read_csv(file_path, infer_schema=False)
    return dataframe.rename({col: clean_column_name(col) for col in dataframe.columns})


def read_route_metadata(file_path: Path) -> pl.DataFrame:
    """Achata o JSON de uma rota inferida em uma linha por aresta.

    :param file_path: Caminho do JSON, com as chaves ``route_id``, ``edges`` e ``edge_geometry``.
    :returns: DataFrame de texto com ``route_id, edge_order, u, v, key, geometry, length``.
    """
    content = json.loads(file_path.read_text(encoding="utf-8"))
    edge_geometry = content.get("edge_geometry", {})

    rows = []
    for edge in content["edges"]:
        geometry = edge_geometry.get(f"({edge['u']},{edge['v']},{edge['key']})", {})
        rows.append(
            {
                "route_id": content["route_id"],
                "edge_order": edge["edge_order"],
                "u": edge["u"],
                "v": edge["v"],
                "key": edge["key"],
                "geometry": geometry.get("geometry"),
                "length": geometry.get("length"),
            }
        )

    return pl.DataFrame(rows).cast(pl.String)


def reference_column_order(dataframes: dict[Path, pl.DataFrame]) -> list[str]:
    """Escolhe a ordem de colunas mais comum e valida que todos os arquivos têm as mesmas colunas.

    Alguns arquivos trazem as mesmas colunas em outra ordem, o que geraria schemas de Parquet
    diferentes para a mesma tabela.

    :param dataframes: DataFrames indexados pelo arquivo de origem.
    :returns: Ordem de colunas a aplicar em todos os arquivos.
    :raises ValueError: Se algum arquivo tiver um conjunto de colunas diferente.
    """
    orders = Counter(tuple(df.columns) for df in dataframes.values())
    reference = list(orders.most_common(1)[0][0])

    mismatched = [path.name for path, df in dataframes.items() if set(df.columns) != set(reference)]
    if mismatched:
        raise ValueError(f"Arquivos com conjunto de colunas diferente de {reference}: {mismatched[:5]}")

    if len(orders) > 1:
        reordered = sum(1 for df in dataframes.values() if list(df.columns) != reference)
        logger.info("%d arquivo(s) com colunas em outra ordem; padronizados para a ordem mais comum", reordered)
    return reference


def download_files(bucket_name: str, source_prefix: str, table_id: str, project_id: str | None = None) -> Path:
    """Baixa do GCS, em paralelo, os arquivos de origem de uma tabela para um diretório temporário.

    :param bucket_name: Nome do bucket do GCS.
    :param source_prefix: Prefixo raiz do dataset no bucket.
    :param table_id: Tabela a carregar. Deve ser uma chave de ``TABLES``.
    :param project_id: Projeto do GCP. Se ``None``, usa o padrão do ambiente.
    :returns: Diretório com os arquivos baixados.
    :raises ValueError: Se ``table_id`` não for suportado.
    :raises FileNotFoundError: Se nenhum arquivo correspondente for encontrado no bucket.
    """
    config = get_table_config(table_id)
    prefix = f"{source_prefix.rstrip('/')}/{config.path}"
    is_folder = prefix.endswith("/")

    client = storage.Client(project=project_id)
    blobs = [
        blob
        for blob in client.list_blobs(bucket_name, prefix=prefix)
        if blob.name.lower().endswith(f".{config.file_format}") and (is_folder or blob.name == prefix)
    ]
    if not blobs:
        raise FileNotFoundError(f"Nenhum arquivo '.{config.file_format}' em 'gs://{bucket_name}/{prefix}'")

    download_dir = Path(tempfile.mkdtemp(prefix=f"rj_smct__mit_{table_id}_src_"))
    logger.info("Baixando %d arquivo(s) de 'gs://%s/%s' para '%s'", len(blobs), bucket_name, prefix, download_dir)
    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as executor:
        # list() força a avaliação para que erros de download sejam levantados aqui.
        list(executor.map(lambda blob: blob.download_to_filename(download_dir / Path(blob.name).name), blobs))

    return download_dir


def convert_to_parquet(download_dir: Path, table_id: str) -> Path:
    """Converte os arquivos baixados de uma tabela para Parquet, um por arquivo de origem.

    Tabelas com ``parse_capture_name`` ganham as colunas ``source_file, capture_date, capture_period,
    capture_duration_min`` (e ``route_id``, quando ausente) extraídas do nome do arquivo.

    :param download_dir: Diretório com os arquivos de origem.
    :param table_id: Tabela a carregar. Deve ser uma chave de ``TABLES``.
    :returns: Diretório com os arquivos Parquet, pronto para upload.
    :raises ValueError: Se ``table_id`` não for suportado ou os arquivos tiverem colunas diferentes.
    :raises FileNotFoundError: Se ``download_dir`` não tiver arquivos do formato esperado.
    """
    config = get_table_config(table_id)
    files = sorted(Path(download_dir).glob(f"*.{config.file_format}"))
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo '.{config.file_format}' em '{download_dir}'")

    output_dir = Path(tempfile.mkdtemp(prefix=f"rj_smct__mit_{table_id}_"))
    logger.info("Convertendo %d arquivo(s) '.%s' para Parquet em '%s'", len(files), config.file_format, output_dir)

    dataframes: dict[Path, pl.DataFrame] = {}
    for file_path in files:
        dataframe = read_csv_as_strings(file_path) if config.file_format == "csv" else read_route_metadata(file_path)

        if config.parse_capture_name:
            parsed = parse_capture_name(file_path.stem)
            extra = {"source_file": file_path.name, **{k: v for k, v in parsed.items() if k not in dataframe.columns}}
            dataframe = dataframe.with_columns(
                [pl.lit(value, dtype=pl.String).alias(name) for name, value in extra.items()]
            )
        dataframes[file_path] = dataframe

    column_order = reference_column_order(dataframes)
    for file_path, dataframe in dataframes.items():
        dataframe.select(column_order).write_parquet(output_dir / f"{file_path.stem}.parquet")

    logger.info("%d arquivo(s) Parquet gravado(s)", len(files))
    return output_dir
