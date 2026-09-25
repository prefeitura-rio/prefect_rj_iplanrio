"""Testes do utils.py do rj_rmi__run_dbt."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
IMPORT_UTILS = """
import sys
import pipelines.rj_rmi__run_dbt.utils
loaded = sorted(name for name in sys.modules if name.split(".")[0] == "prefect")
assert not loaded, loaded
"""


def test_utils_imports_without_prefect() -> None:
    """Confere que importar o ``utils.py`` não carrega o Prefect (§4.3)."""
    result = subprocess.run(
        [sys.executable, "-c", IMPORT_UTILS],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
