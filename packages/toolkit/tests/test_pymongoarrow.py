from subprocess import run
from sys import executable


def test_regression_pymongoarrow_imports_without_a_warning() -> None:
    """Regression: pyarrow 25.0.1 grew `Array`, so pymongoarrow warned at import."""
    result = run(
        [executable, "-W", "error::RuntimeWarning", "-c", "import pymongoarrow.api"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
