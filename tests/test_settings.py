import pytest
from pydantic import ValidationError

from utils.settings import SfmcSettings, WetalkieSettings

WETALKIE = {"WETALKIE_URL": "https://wetalkie.test", "WETALKIE_USERNAME": "user", "WETALKIE_PASSWORD": "secret"}
SFMC = {
    "SFMC_AUTH_URL": "https://auth.test",
    "SFMC_CLIENT_ID": "id",
    "SFMC_CLIENT_SECRET": "secret",
    "SFMC_REST_BASE_URL": "https://rest.test",
}


@pytest.mark.parametrize(
    ("group", "variables"),
    [
        pytest.param(WetalkieSettings, WETALKIE, id="wetalkie"),
        pytest.param(SfmcSettings, SFMC, id="sfmc"),
    ],
)
def test_a_group_reads_its_variables_and_hides_the_secret(monkeypatch, group, variables):
    """Read every variable of the group and keep the secret out of the repr."""
    for name, value in variables.items():
        monkeypatch.setenv(name, value)

    settings = group()

    assert settings.model_dump(include=set(variables)).keys() == variables.keys()
    assert "secret" not in repr(settings)


@pytest.mark.parametrize(
    ("group", "variables", "missing"),
    [
        pytest.param(WetalkieSettings, WETALKIE, "WETALKIE_PASSWORD", id="wetalkie-without-a-password"),
        pytest.param(SfmcSettings, SFMC, "SFMC_AUTH_URL", id="sfmc-without-an-auth-url"),
    ],
)
def test_a_missing_variable_names_itself(monkeypatch, group, variables, missing):
    """Fail with the name of the variable that the environment lacks."""
    for name, value in variables.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv(missing)

    with pytest.raises(ValidationError, match=missing):
        group()
