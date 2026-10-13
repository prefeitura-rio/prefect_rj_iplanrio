import pytest

from toolkit.settings import Settings, read_database_credentials


def test_read_database_credentials(monkeypatch):
    monkeypatch.setenv("DB_ORACLE_X__DB_USERNAME", "user")
    monkeypatch.setenv("DB_ORACLE_X__DB_PASSWORD", "secret")
    credentials = read_database_credentials("/db-oracle-x")
    assert (credentials.username, credentials.password) == ("user", "secret")
    assert "secret" not in repr(credentials)


def test_missing_credentials_raise(monkeypatch):
    monkeypatch.delenv("NOPE__DB_USERNAME", raising=False)
    with pytest.raises(KeyError):
        read_database_credentials("nope")


def test_hmac_secret_is_hidden():
    settings = Settings(GCS_HMAC_SECRET="top-secret")  # pyright: ignore[reportArgumentType]
    assert "top-secret" not in repr(settings)
    assert settings.GCS_HMAC_SECRET.get_secret_value() == "top-secret"
