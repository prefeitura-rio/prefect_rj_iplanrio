import pytest

from toolkit.settings import read_database_credentials, read_secret


def test_a_secret_is_every_variable_of_its_path_by_the_short_name(monkeypatch):
    """Read the variables of `/api-x`, and no variable of another path."""
    monkeypatch.setenv("API_X__TOKEN", "t")
    monkeypatch.setenv("API_X__USER", "u")
    monkeypatch.setenv("API_XY__TOKEN", "other")

    assert read_secret("/api-x") == {"TOKEN": "t", "USER": "u"}


def test_a_secret_with_no_variable_is_a_key_error(monkeypatch):
    """Raise, so a typo in the path fails at once and not at the first request."""
    monkeypatch.delenv("API_Z__TOKEN", raising=False)

    with pytest.raises(KeyError):
        read_secret("/api-z")


def test_the_credentials_of_a_database_are_its_user_and_its_password(monkeypatch):
    """Read `DB_USERNAME` and `DB_PASSWORD` of the path."""
    monkeypatch.setenv("DB_ORACLE_X__DB_USERNAME", "u")
    monkeypatch.setenv("DB_ORACLE_X__DB_PASSWORD", "p")

    credentials = read_database_credentials("/db-oracle-x")

    assert (credentials.username, credentials.password) == ("u", "p")


def test_a_secret_without_the_password_is_a_key_error(monkeypatch):
    """Raise `KeyError` when only the user is set."""
    monkeypatch.setenv("DB_ONLY_USER__DB_USERNAME", "u")
    monkeypatch.delenv("DB_ONLY_USER__DB_PASSWORD", raising=False)

    with pytest.raises(KeyError):
        read_database_credentials("/db-only-user")
