import pytest
from pydantic import TypeAdapter, ValidationError

from utils.types import DatabaseDumpOptions, DumpOptions


@pytest.mark.parametrize("options", [DumpOptions, DatabaseDumpOptions])
@pytest.mark.parametrize(
    "key",
    [
        pytest.param("batches", id="the_old_name_of_chunks"),
        pytest.param("filter", id="a_misspelled_key"),
    ],
)
def test_regression_an_option_that_does_not_exist_is_refused(options, key):
    """Regression: a renamed or misspelled option once fell back to the default.

    The dump then loaded the whole table as one chunk, with no error.
    """
    with pytest.raises(ValidationError, match=key):
        TypeAdapter(options).validate_python({key: {"column": "id", "width": 1, "begin": 0}})
