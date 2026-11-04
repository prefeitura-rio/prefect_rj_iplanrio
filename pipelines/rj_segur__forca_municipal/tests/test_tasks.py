import pytest

from pipelines.rj_segur__forca_municipal import tasks
from pipelines.rj_segur__forca_municipal.tasks import read_unit_ids, run_qmd_task
from utils.types import Cloud, Table

CLOUD = Cloud(bucket="bucket", project="project", connection="project.us.biglake")
DATASET = "brutos_forca_municipal"


@pytest.fixture
def ids_read(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str, str, str]]:
    """Record the reads of ids and answer with no id."""
    calls: list[tuple[str, str, str, str]] = []

    async def fake(project: str, table: str, name: str, column: str) -> list[str]:
        calls.append((project, table, name, column))
        return []

    monkeypatch.setattr(tasks, "read_ids_task", fake)

    return calls


async def test_regression_the_unit_ids_come_from_the_staging_table(ids_read: list[tuple[str, str, str, str]]) -> None:
    """Regression: the table had no `_staging`, so the dataset did not exist."""
    table = Table(dataset_id=DATASET, table_id="unit_positions")

    await read_unit_ids(table, CLOUD)

    assert ids_read == [("project", f"project.{DATASET}_staging.unidades_historico", "unit_ids", "UnitId")]


@pytest.mark.parametrize("table_id", ["qmd_detalhes", "qmd_kml"])
async def test_regression_the_qmd_ids_come_from_the_staging_table(
    ids_read: list[tuple[str, str, str, str]], table_id: str
) -> None:
    """Regression: the table had no `_staging`, so the dataset did not exist."""
    table = Table(dataset_id=DATASET, table_id=table_id)

    await run_qmd_task.fn(table, CLOUD, "append", {})

    assert ids_read == [("project", f"project.{DATASET}_staging.qmd", "qmd_ids", "Id")]
