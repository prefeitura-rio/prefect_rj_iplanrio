"""Configuração do Postgres: o DSN vem do ambiente e a falta dele é um erro explícito, antes de qualquer conexão."""

import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import postgres


def test_the_dsn_is_read_from_the_named_environment_variable():
    assert postgres.read_dsn("PG_X__DSN", {"PG_X__DSN": " postgresql://u@h/db "}) == "postgresql://u@h/db"


@pytest.mark.parametrize("environ", [{}, {"PG_X__DSN": ""}, {"PG_X__DSN": "   "}])
def test_a_missing_or_blank_dsn_names_the_variable(environ: dict[str, str]):
    with pytest.raises(postgres.PostgresConfigError, match="PG_X__DSN"):
        postgres.read_dsn("PG_X__DSN", environ)


def test_the_error_never_shows_the_dsn():
    with pytest.raises(postgres.PostgresConfigError) as error:
        postgres.read_dsn("PG_X__DSN", {"OUTRA": "postgresql://u:segredo@h/db"})

    assert "segredo" not in str(error.value)
