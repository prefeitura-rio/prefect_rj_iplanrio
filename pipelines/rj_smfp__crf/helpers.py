"""Helpers of rj_smfp__crf."""

from collections.abc import Sequence
from datetime import date
from fnmatch import fnmatch
from pathlib import Path
from re import IGNORECASE, search
from tempfile import TemporaryDirectory
from zipfile import BadZipFile, ZipFile

import polars as pl
from toolkit.gcs import create_store, download, list_sizes
from toolkit.telemetry import logger

from pipelines.rj_smfp__crf.constants import EXTRA_COLUMNS, LAYOUTS, NAME_PARTS, ZIP_PREFIX
from pipelines.rj_smfp__crf.types import FwfLayout

type Row = dict[str, str | None]


def parse_date(value: date | str | None, name: str) -> date | None:
    """Read a date given as `date` or as `YYYY-MM-DD`, and keep nothing as nothing.

    Raise `ValueError` for a text that is not a date, naming the parameter.
    """
    if value is None or isinstance(value, date):
        return value

    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"Formato de data inválido para '{name}': '{value}'. Use o formato YYYY-MM-DD.") from None


def date_from_name(name: str) -> date | None:
    """Read the date `DDMMYYYY` that ends a ZIP name, such as `...-03012021.zip`.

    Return nothing if the name has no such date, or the date does not exist.
    """
    found = search(r"-(\d{8})\.zip$", name.rsplit("/", 1)[-1], flags=IGNORECASE)

    if found is None:
        return None

    text = found[1]

    try:
        return date(int(text[4:8]), int(text[2:4]), int(text[0:2]))
    except ValueError:
        logger.warning("Data inválida extraída de '%s': %s", name, text)

        return None


def select_zip_names(names: Sequence[str], start: date | None, end: date | None) -> list[str]:
    """Keep the ZIP files of the CRF whose date is after `start` and up to `end`.

    A file on `start` or before it is skipped, as the watermark says it was loaded, and
    a file after `end` is skipped. With neither limit all the ZIP files stay. A name
    whose date cannot be read is skipped.
    """
    zips = [name for name in names if name.rsplit("/", 1)[-1].startswith(ZIP_PREFIX) and name.endswith(".zip")]

    if start is None and end is None:
        return zips

    kept: list[str] = []

    for name in zips:
        moment = date_from_name(name)

        if moment is None:
            logger.warning("Não foi possível extrair data de '%s', ignorando", name)
        elif (start is None or moment > start) and (end is None or moment <= end):
            kept.append(name)

    return kept


def read_fwf(text: str, layout: FwfLayout) -> list[Row]:
    """Cut each line of a text into the columns of the layout.

    A field is stripped, and an empty one is nothing. A blank line is skipped.
    """
    rows: list[Row] = []

    for line in text.splitlines():
        if not line.strip():
            continue

        fields = (line[start:end].strip() for start, end in layout.colspecs)
        rows.append({name: value or None for name, value in zip(layout.names, fields, strict=True)})

    return rows


def reference_date(file_name: str) -> date:
    """Read the date `YYYYMMDD` that ends a file name, such as `00-PER-20210103.txt`.

    Raise `ValueError` for a name that does not follow `XX-XXX-YYYYMMDD.txt`.
    """
    parts = file_name.rsplit(".", 1)[0].split("-")

    if len(parts) < NAME_PARTS:
        raise ValueError(f"Nome do arquivo '{file_name}' não segue o padrão 'XX-XXX-AAAAMMDD.txt'")

    text = parts[-1]

    return date(int(text[0:4]), int(text[4:6]), int(text[6:8]))


def build_rows(file_name: str, text: str, layout: FwfLayout) -> list[Row]:
    """Read a file of the CRF as rows, with its origin and its day of reference.

    Each row has `arquivo_origem`, `data_referencia` (`YYYYMMDD`), `ano_particao`,
    `mes_particao` and `data_particao` (`YYYY-MM-DD`).
    """
    moment = reference_date(file_name)
    extra: Row = {
        "arquivo_origem": file_name,
        "data_referencia": f"{moment:%Y%m%d}",
        "ano_particao": f"{moment:%Y}",
        "mes_particao": f"{moment:%m}",
        "data_particao": moment.isoformat(),
    }

    return [{**row, **extra} for row in read_fwf(text, layout)]


def read_member(archive: ZipFile, layout: FwfLayout, table_id: str) -> tuple[str, str]:
    """Read the one file of the ZIP that matches the layout, as its name and its text.

    Raise `FileNotFoundError` if none matches, and `ValueError` if more than one does.
    """
    found = [name for name in archive.namelist() if fnmatch(Path(name).name, layout.file_pattern)]

    if not found:
        raise FileNotFoundError(f"Nenhum arquivo com padrão '{layout.file_pattern}' para a tabela '{table_id}'")

    if len(found) > 1:
        raise ValueError(f"Múltiplos arquivos com padrão '{layout.file_pattern}' para a tabela '{table_id}': {found}")

    return Path(found[0]).name, archive.read(found[0]).decode(layout.encoding)


def process_zip(bucket: str, name: str, table_id: str, folder: Path) -> int:
    """Download a ZIP, read its file of the table, and write one Parquet file.

    Return the rows. Raise `zipfile.BadZipFile` for a file that is not a ZIP.
    """
    layout = LAYOUTS[table_id]

    with TemporaryDirectory() as directory:
        path = download(create_store(bucket), name, Path(directory) / Path(name).name)

        try:
            with ZipFile(path) as archive:
                file_name, text = read_member(archive, layout, table_id)
        except BadZipFile as error:
            raise BadZipFile(f"File '{name}' is not a valid ZIP file") from error

    rows = build_rows(file_name, text, layout)
    schema = dict.fromkeys([*layout.names, *EXTRA_COLUMNS], pl.String)
    pl.DataFrame(rows, schema=schema).write_parquet(folder / f"{Path(name).stem}.parquet")

    return len(rows)


def list_names(bucket: str, prefix: str) -> list[str]:
    """List the names of the files under the prefix of the bucket."""
    return list(list_sizes(create_store(bucket), prefix))
