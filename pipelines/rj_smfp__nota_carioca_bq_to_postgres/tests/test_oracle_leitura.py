"""Oracle somente leitura: as cinco variáveis ``DB_ORACLE_NC_LEITURA__*`` e o erro explícito quando faltam."""

import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import oracle_leitura

PATH = "/db-oracle-nc-leitura"
PREFIX = "DB_ORACLE_NC_LEITURA__"
KEYS = ("DB_HOST", "DB_PORT", "DB_SERVICE_NAME", "DB_USERNAME", "DB_PASSWORD")


def environ(**overrides: str | None) -> dict[str, str]:
    values = {
        "DB_HOST": "oracle.test",
        "DB_PORT": "1521",
        "DB_SERVICE_NAME": "svc",
        "DB_USERNAME": "leitor",
        "DB_PASSWORD": "x",
    }
    values.update({key: value for key, value in overrides.items() if value is not None})
    return {f"{PREFIX}{key}": value for key, value in values.items() if overrides.get(key, "") is not None}


def test_the_variables_come_from_the_infisical_path_convention():
    config = oracle_leitura.read_config(PATH, environ())

    assert (config.host, config.port, config.service_name, config.user) == ("oracle.test", 1521, "svc", "leitor")


def test_the_error_names_every_missing_variable_and_nothing_else():
    with pytest.raises(oracle_leitura.OracleLeituraConfigError) as error:
        oracle_leitura.read_config(
            PATH, {f"{PREFIX}DB_HOST": "oracle.test", f"{PREFIX}DB_PASSWORD": "segredo-de-teste"}
        )

    message = str(error.value)
    assert all(f"{PREFIX}{key}" in message for key in ("DB_PORT", "DB_SERVICE_NAME", "DB_USERNAME"))
    assert f"{PREFIX}DB_HOST" not in message
    assert "segredo-de-teste" not in message


def test_a_completely_unset_read_user_is_an_error_not_a_silent_skip():
    with pytest.raises(oracle_leitura.OracleLeituraConfigError, match=f"{PREFIX}DB_USERNAME"):
        oracle_leitura.read_config(PATH, {})


def test_the_port_must_be_a_number():
    with pytest.raises(oracle_leitura.OracleLeituraConfigError, match=f"{PREFIX}DB_PORT"):
        oracle_leitura.read_config(PATH, environ(DB_PORT="listener"))


def test_another_secret_path_reads_other_variables():
    other = {key.replace(PREFIX, "DB_ORACLE_OUTRO__"): value for key, value in environ().items()}

    assert oracle_leitura.read_config("/db-oracle-outro", other).host == "oracle.test"
