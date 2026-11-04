from datetime import datetime
from decimal import Decimal

import pytest

from pipelines.rj_smfp__nota_carioca_extracao_xml.helpers.format import format_duration
from pipelines.rj_smfp__nota_carioca_extracao_xml.helpers.oracle import build_row
from pipelines.rj_smfp__nota_carioca_extracao_xml.types import TaskRow

START = datetime(2026, 10, 20, 12, 0, 0)


def test_a_row_of_the_database_becomes_a_task() -> None:
    """Convert the numbers, the dates and the trimmed summary."""
    record: dict[str, object] = {
        "tarefa": Decimal(12),
        "tipo": 46,
        "status": Decimal(1),
        "data_inicio": START,
        "data_termino": None,
        "execucao_progresso": Decimal(40),
        "execucao_resumo": "  em andamento \n",
        "segundos_em_execucao": Decimal("123"),
    }

    assert build_row(record) == TaskRow(12, 46, 1, START, None, 40, "em andamento", 123)


def test_a_row_with_only_the_required_columns_has_empty_options() -> None:
    """Leave the dates, the progress and the summary empty when they are null."""
    task = build_row({"tarefa": 1, "tipo": 48, "status": 2, "execucao_resumo": "  "})

    assert (task.started_at, task.progress, task.summary, task.running_seconds) == (None, None, None, None)


@pytest.mark.parametrize(
    "record",
    [
        pytest.param({"tarefa": None, "tipo": 46, "status": 1}, id="no_number"),
        pytest.param({"tarefa": 1, "tipo": 46, "status": 1, "data_inicio": "yesterday"}, id="text_date"),
        pytest.param({"tarefa": object(), "tipo": 46, "status": 1}, id="not_a_number"),
    ],
)
def test_a_row_that_does_not_fit_is_refused(record: dict[str, object]) -> None:
    """Raise `TypeError` for a missing key column or a column of the wrong type."""
    with pytest.raises(TypeError):
        build_row(record)


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        pytest.param(-5, "0s", id="negative"),
        pytest.param(12, "12s", id="seconds"),
        pytest.param(245, "4min 05s", id="minutes"),
        pytest.param(3720, "1h 02min", id="hours"),
    ],
)
def test_a_duration_is_written_in_its_largest_unit(seconds: int, expected: str) -> None:
    """Write seconds, minutes with seconds, or hours with minutes."""
    assert format_duration(seconds) == expected
