import pytest

from toolkit.settings import read_database_credentials


@pytest.mark.parametrize(
    ("secret_path", "prefix"),
    [
        pytest.param("/db-oracle-x", "DB_ORACLE_X", id="slashes_and_dashes"),
        pytest.param("db-x", "DB_X", id="no_leading_slash"),
        pytest.param("/DB_Y", "DB_Y", id="already_upper_case"),
    ],
)
def test_the_credentials_are_read_from_the_variables_of_the_secret_path(
    monkeypatch, secret_path, prefix
):
    monkeypatch.setenv(f"{prefix}__DB_USERNAME", "user")
    monkeypatch.setenv(f"{prefix}__DB_PASSWORD", "secret")

    credentials = read_database_credentials(secret_path)

    assert (credentials.username, credentials.password) == ("user", "secret")


def test_a_missing_variable_is_an_error(monkeypatch):
    monkeypatch.delenv("NOPE__DB_USERNAME", raising=False)

    with pytest.raises(KeyError):
        read_database_credentials("nope")
