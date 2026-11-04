import pytest
from pydantic import ValidationError

from toolkit.settings import (
    CouchDBSettings,
    DatabaseSettings,
    GcsSettings,
    ToolkitSettings,
)


def test_a_bad_value_of_one_group_does_not_break_another(monkeypatch) -> None:
    """Read the CouchDB settings although a GCS variable is wrong."""
    monkeypatch.setenv("GCS_USE_SSL", "maybe")

    assert CouchDBSettings().COUCHDB_PAGE_SIZE == 10_000

    with pytest.raises(ValidationError, match="GCS_USE_SSL"):
        GcsSettings()


def test_every_group_shares_the_log_level_and_the_telemetry(monkeypatch) -> None:
    """Give the log level and the telemetry to every group from the base."""
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "toolkit-test")

    groups = [kind() for kind in (ToolkitSettings, GcsSettings, CouchDBSettings)]

    assert [(group.LOG_LEVEL, group.OTEL_SERVICE_NAME) for group in groups] == [
        ("DEBUG", "toolkit-test")
    ] * 3


@pytest.mark.parametrize("kind", [ToolkitSettings, GcsSettings, CouchDBSettings])
def test_a_bad_shared_value_breaks_every_group(monkeypatch, kind) -> None:
    """Raise in every group for a wrong log level, because all of them share it."""
    monkeypatch.setenv("LOG_LEVEL", "trace")

    with pytest.raises(ValidationError, match="LOG_LEVEL"):
        kind()


def test_the_database_settings_hold_the_user_and_the_password(monkeypatch) -> None:
    """Read the login of the database, and keep the password out of the repr."""
    monkeypatch.setenv("DB_USERNAME", "reader")
    monkeypatch.setenv("DB_PASSWORD", "segredo")

    settings = DatabaseSettings()

    assert (settings.DB_USERNAME, settings.DB_PASSWORD.get_secret_value()) == (
        "reader",
        "segredo",
    )
    assert "segredo" not in repr(settings)


@pytest.mark.parametrize("missing", ["DB_USERNAME", "DB_PASSWORD"])
def test_a_database_login_that_lacks_a_variable_names_it(monkeypatch, missing) -> None:
    """Raise for a missing user or password, and name the variable that is missing."""
    monkeypatch.setenv("DB_USERNAME", "reader")
    monkeypatch.setenv("DB_PASSWORD", "segredo")
    monkeypatch.delenv(missing)

    with pytest.raises(ValidationError, match=missing):
        DatabaseSettings()
