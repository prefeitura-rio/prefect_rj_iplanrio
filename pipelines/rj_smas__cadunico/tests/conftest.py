from collections.abc import Callable
from pathlib import Path
from typing import cast
from zipfile import ZipFile

import pytest
from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet

from pipelines.rj_smas__cadunico.helpers import prepare
from pipelines.rj_smas__cadunico.types import Cells, Record

TABLE_HEADER = [
    "REG",
    "Campo",
    "Arquivo base",
    "Posição",
    "Tamanho",
    "Tipo",
    "Transformação",
    "Descrição",
    "Nulos",
    "Obs",
]


@pytest.fixture
def make_record() -> Callable[..., Record]:
    """Return a factory of rows of the layout joined to the dictionary of columns."""

    def build(
        column: str, version: str = "7.01", *, reg: str = "1", particao: str = "0701", **fields: str | None
    ) -> Record:
        """Build a row with the keys that the models read."""
        return {
            "table": "REG_01",
            "version": version,
            "reg": reg,
            "campo": None,
            "arquivo_base_versao_7": column,
            "posicao": "10",
            "tamanho": "3",
            "tipo": None,
            "transformacao": None,
            "descricao": "Descricao. Outra frase",
            "nulos": None,
            "observacoes": None,
            "column": column,
            "versao_layout_particao": particao,
            "versao_layout_anterior": particao,
            "coluna_esta_versao_anterior": "True",
            "nome_padronizado": f"nome_{column}",
            "bigquery_type": "STRING",
            "date_format": None,
            "ajuste_decimal": None,
            "dicionario_atributos": None,
            **fields,
        }

    return build


@pytest.fixture
def make_workbook(tmp_path: Path) -> Callable[[dict[str, Cells]], Path]:
    """Return a factory of spreadsheets, from the cells of each sheet."""

    def build(sheets: dict[str, Cells]) -> Path:
        """Write the sheets in a `.xlsx` file and return its path."""
        workbook = Workbook()
        del workbook["Sheet"]

        for name, cells in sheets.items():
            sheet = cast("Worksheet", workbook.create_sheet(name))

            for row in cells:
                sheet.append(row)

        path = tmp_path / "BMM_V7_01.xlsx"
        workbook.save(path)

        return path

    return build


@pytest.fixture
def make_archive(tmp_path: Path) -> Callable[[str, dict[str, str]], Path]:
    """Return a factory of ZIP files, from the text of each file inside."""

    def build(name: str, texts: dict[str, str]) -> Path:
        """Write the ZIP in a folder of the test and return its path."""
        path = tmp_path / "source" / name
        path.parent.mkdir(exist_ok=True)

        with ZipFile(path, "w") as archive:
            for entry, text in texts.items():
                archive.writestr(entry, text)

        return path

    return build


@pytest.fixture
def serve_archive(monkeypatch: pytest.MonkeyPatch) -> Callable[[Path], None]:
    """Return a function that makes the download of any file give the ZIP at a path."""

    def serve(archive: Path) -> None:
        """Replace the download of `prepare_partition`."""

        def download(bucket: str, name: str, folder: Path) -> Path:
            """Give the ZIP, and keep the bucket, the name and the folder."""
            del bucket, name, folder

            return archive

        monkeypatch.setattr(prepare, "download_blob", download)

    return serve
