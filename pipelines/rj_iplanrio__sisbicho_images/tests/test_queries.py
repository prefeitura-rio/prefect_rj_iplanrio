# -*- coding: utf-8 -*-
"""SQL da pipeline: tudo em queries/, renderizado por load_query (string.Template, STYLEGUIDE §7)."""

import re
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from google.cloud.exceptions import NotFound

from pipelines.rj_iplanrio__sisbicho_images import tasks
from pipelines.rj_iplanrio__sisbicho_images.constants import QUERIES_ANCHOR

PIPELINE = Path(QUERIES_ANCHOR).parent
QUERIES = sorted((PIPELINE / "queries").glob("*.sql"))
ORIGEM, DESTINO = "proj.brutos_sisbicho.animal", "proj.brutos_sisbicho.animal_imagem"


@pytest.fixture(autouse=True)
def _sem_log_do_prefect():
    with mock.patch.object(tasks, "log"):
        yield


def _cliente(destino_existe: bool) -> mock.MagicMock:
    c = mock.MagicMock()
    if destino_existe:
        c.get_table.return_value.num_rows = 5
    else:
        c.get_table.side_effect = NotFound("destino")
    c.query.return_value.result.return_value = iter([SimpleNamespace(total=3)])
    return c


def _sqls(c: mock.MagicMock) -> list[str]:
    return [chamada.args[0] for chamada in c.query.call_args_list]


def test_queries_existem():
    assert {q.stem for q in QUERIES} == {
        "conta_linhas",
        "conta_pendentes_incremental",
        "conta_pendentes_carga_completa",
        "busca_lote_incremental",
        "busca_lote_carga_completa",
    }


@pytest.mark.parametrize("arquivo", QUERIES, ids=lambda q: q.stem)
def test_query_sem_placeholder_de_str_format(arquivo: Path):
    assert not re.search(r"(?<!\$)\{\w+\}", arquivo.read_text(encoding="utf-8"))


def test_tasks_nao_tem_sql_inline():
    assert not re.search(r"f[\"']+\s*(SELECT|WITH)\b", (PIPELINE / "tasks.py").read_text(encoding="utf-8"))


def test_conta_linhas():
    c = _cliente(destino_existe=True)
    assert tasks._try_query_row_count(c, DESTINO) == 3
    (sql,) = _sqls(c)
    assert sql.strip().endswith(f"SELECT COUNT(*) AS total FROM `{DESTINO}`")


def test_total_incremental_exclui_quem_ja_esta_no_destino():
    c = _cliente(destino_existe=True)
    assert tasks._get_total_count(c, ORIGEM, DESTINO, "id_animal") == (3, False)
    (sql,) = _sqls(c)
    assert f"FROM `{ORIGEM}` AS src" in sql
    assert f"LEFT JOIN `{DESTINO}` AS tgt" in sql
    assert "ON CAST(src.id_animal AS STRING) = tgt.id_animal" in sql
    assert "AND tgt.id_animal IS NULL" in sql


def test_total_primeira_carga_nao_olha_o_destino():
    c = _cliente(destino_existe=False)
    assert tasks._get_total_count(c, ORIGEM, DESTINO, "id_animal") == (3, False)
    (sql,) = _sqls(c)
    assert f"FROM `{ORIGEM}` AS src" in sql
    assert DESTINO not in sql


def test_lote_incremental():
    c = _cliente(destino_existe=True)
    c.query.return_value.result.return_value = mock.MagicMock()
    tasks.fetch_batch(c, ORIGEM, DESTINO, "id_animal", offset=100, batch_size=50)
    (sql,) = _sqls(c)
    assert "PARTITION BY id_animal ORDER BY datalake_loaded_at DESC" in sql
    assert f"LEFT JOIN `{DESTINO}` AS tgt" in sql
    assert "LEFT JOIN `proj.brutos_sisbicho.animal_proprietario` AS ap" in sql
    assert "LEFT JOIN `proj.brutos_sisbicho.proprietario` AS prop" in sql
    assert sql.rstrip().endswith("LIMIT 50\nOFFSET 100")


def test_lote_primeira_carga():
    c = _cliente(destino_existe=False)
    c.query.return_value.result.return_value = mock.MagicMock()
    tasks.fetch_batch(c, ORIGEM, DESTINO, "id_animal", offset=0, batch_size=50)
    (sql,) = _sqls(c)
    assert DESTINO not in sql
    assert "LEFT JOIN `proj.brutos_sisbicho.animal_proprietario` AS ap" in sql
    assert sql.rstrip().endswith("LIMIT 50\nOFFSET 0")
