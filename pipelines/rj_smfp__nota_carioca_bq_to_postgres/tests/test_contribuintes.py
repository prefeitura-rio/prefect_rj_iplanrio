"""Sincronização de MV_CONTRIBUINTES (Oracle somente leitura → Postgres), por snapshot e numa transação."""

import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import oracledb
import psycopg
import pytest
from conftest import Database
from harness import CONTRIBUINTES_TABLE, OracleContribuintes

from pipelines.rj_smfp__nota_carioca_bq_to_postgres import flow_contribuintes
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import contribuintes, oracle_leitura, postgres

PG_COLUMNS = "inscricao, cpf_cnpj, inscricao_notanacional, pedagio, categoria, ativo"
FRESH_SECONDS = 120


def taxpayer(number: int, **changes: str | Decimal | None) -> tuple:
    """Um contribuinte sintético: INSCRICAO, CPF_CNPJ, INSCRICAO_NOTANACIONAL, PEDAGIO, CATEGORIA, ATIVO."""
    row = {
        "inscricao": f"INS{number:03d}",
        "cpf_cnpj": f"{number:011d}",
        "nota": f"NN{number:03d}",
        "pedagio": Decimal(number),
        "categoria": Decimal(1),
        "ativo": Decimal(1),
    }
    row.update(changes)
    return tuple(row.values())


@pytest.fixture
def oracle(oracle_contribuintes: OracleContribuintes) -> OracleContribuintes:
    oracle_contribuintes.replace_rows([])
    return oracle_contribuintes


@pytest.fixture
def sync(database: Database, oracle: OracleContribuintes) -> Callable[[], contribuintes.SyncResult]:
    """Uma execução do sincronizador como a do flow: Oracle somente leitura e o login do papel no Postgres."""

    def run() -> contribuintes.SyncResult:
        config = oracle_leitura.read_config(environ=oracle.read_env())
        with (
            oracle_leitura.open_read_only(config) as source,
            postgres.connect(database.sincronizador_dsn, "teste") as target,
        ):
            return contribuintes.sync_contribuintes(source, target, CONTRIBUINTES_TABLE)

    return run


def pg_rows(database: Database) -> list[tuple]:
    with database.admin() as admin:
        return admin.execute(f"SELECT {PG_COLUMNS} FROM public.mv_contribuintes ORDER BY inscricao").fetchall()


class TestSyncContribuintes:
    def test_sync_contribuintes_applies_an_insert_an_update_and_a_removal(self, database, oracle, sync):
        # Given: três contribuintes já sincronizados
        oracle.replace_rows([taxpayer(1), taxpayer(2), taxpayer(3)])
        sync()

        # When: no Oracle entra o 4, o 2 muda de CPF/CNPJ e o 3 some; roda uma execução
        oracle.replace_rows([taxpayer(1), taxpayer(2, cpf_cnpj="99999999999"), taxpayer(4)])
        result = sync()

        # Then: o Postgres reflete o Oracle e o resultado conta cada tipo de mudança
        assert pg_rows(database) == [taxpayer(1), taxpayer(2, cpf_cnpj="99999999999"), taxpayer(4)]
        assert (result.inserted, result.updated, result.deleted, result.total) == (1, 1, 1, 3)

    def test_sync_contribuintes_first_run_loads_every_row_with_exact_numbers(self, database, oracle, sync):
        exact = Decimal("12345678901234567890.123456789")
        oracle.replace_rows([taxpayer(1, pedagio=Decimal("0.1")), taxpayer(2, pedagio=exact, categoria=None)])

        result = sync()

        rows = pg_rows(database)
        assert [row[3] for row in rows] == [Decimal("0.1"), exact]
        assert rows[1][4] is None
        assert (result.inserted, result.updated, result.deleted) == (2, 0, 0)

    def test_sync_contribuintes_leaves_unchanged_rows_untouched(self, database, oracle, sync):
        oracle.replace_rows([taxpayer(1), taxpayer(2)])
        sync()
        stamps = self._stamps(database)
        time.sleep(1.1)

        oracle.replace_rows([taxpayer(1), taxpayer(2, ativo=Decimal(0))])
        sync()

        after = self._stamps(database)
        assert after["INS001"] == stamps["INS001"]
        assert after["INS002"] > stamps["INS002"]

    @staticmethod
    def _stamps(database: Database) -> dict[str, datetime]:
        with database.admin() as admin:
            return dict(admin.execute("SELECT inscricao, _sincronizado_em FROM public.mv_contribuintes").fetchall())

    def test_sync_contribuintes_records_the_synced_at_stamp_on_every_new_row(self, database, oracle, sync):
        oracle.replace_rows([taxpayer(1)])

        sync()

        stamp = self._stamps(database)["INS001"]
        assert abs(stamp - datetime.now(UTC)) < timedelta(seconds=FRESH_SECONDS)

    def test_sync_contribuintes_writes_the_watermark_row_with_the_oracle_scn(self, database, oracle, sync):
        oracle.replace_rows([taxpayer(1)])
        before = oracle.current_scn()

        result = sync()
        after = oracle.current_scn()

        with database.admin() as admin:
            row = admin.execute(
                "SELECT scn, commit_ts, aplicado_em, heartbeat_ts, heartbeat_aplicado_em "
                "FROM public.cdc_marca_dagua WHERE fonte = 'mv_contribuintes'"
            ).fetchone()
        scn, commit_ts, applied_at, heartbeat_ts, heartbeat_applied_at = row
        assert before <= scn <= after
        assert scn == result.scn
        now = datetime.now(UTC)
        assert all(
            abs(moment - now) < timedelta(seconds=FRESH_SECONDS) for moment in (commit_ts, applied_at, heartbeat_ts)
        )
        assert abs(heartbeat_applied_at - applied_at) < timedelta(seconds=FRESH_SECONDS)

    def test_sync_contribuintes_keeps_one_watermark_row_across_runs(self, database, oracle, sync):
        oracle.replace_rows([taxpayer(1)])
        sync()
        first = self._watermark(database)

        sync()

        second = self._watermark(database)
        assert second[1] >= first[1]
        assert second[2] > first[2]

    @staticmethod
    def _watermark(database: Database) -> tuple:
        with database.admin() as admin:
            rows = admin.execute(
                "SELECT count(*) OVER (), scn, aplicado_em FROM public.cdc_marca_dagua WHERE fonte = 'mv_contribuintes'"
            ).fetchall()
        assert [row[0] for row in rows] == [1]
        return rows[0]

    def test_sync_contribuintes_refuses_an_empty_snapshot_instead_of_wiping_the_table(self, database, oracle, sync):
        oracle.replace_rows([taxpayer(1), taxpayer(2)])
        sync()
        oracle.replace_rows([])

        with pytest.raises(contribuintes.EmptySnapshotError):
            sync()

        assert pg_rows(database) == [taxpayer(1), taxpayer(2)]

    def test_sync_contribuintes_is_one_transaction_so_a_failure_changes_nothing(self, database, oracle, sync):
        oracle.replace_rows([taxpayer(1), taxpayer(2)])
        sync()
        with database.admin() as admin:
            admin.execute("ALTER TABLE public.mv_contribuintes ADD CONSTRAINT sem_ruim CHECK (inscricao <> 'RUIM')")
        oracle.replace_rows([taxpayer(1, ativo=Decimal(0)), taxpayer(3, inscricao="RUIM")])

        with pytest.raises(psycopg.errors.CheckViolation):
            sync()

        assert pg_rows(database) == [taxpayer(1), taxpayer(2)]

    def test_sync_contribuintes_reads_the_oracle_in_a_read_only_session(self, oracle):
        # Given: o usuário de leitura ganha, só neste teste, privilégio de escrita na MV (que nunca terá em produção)
        oracle.execute(f"GRANT INSERT, DELETE ON {CONTRIBUINTES_TABLE} TO NC_LEITURA")
        config = oracle_leitura.read_config(environ=oracle.read_env())

        try:
            # When / Then: com a sessão aberta pelo código de produção, nem com privilégio o Oracle aceita escrever
            with oracle_leitura.open_read_only(config) as connection, connection.cursor() as cursor:
                with pytest.raises(oracledb.DatabaseError, match="ORA-01456"):
                    cursor.execute(f"DELETE FROM {CONTRIBUINTES_TABLE}")
                with pytest.raises(oracledb.DatabaseError, match="ORA-01456"):
                    cursor.execute(f"INSERT INTO {CONTRIBUINTES_TABLE} (INSCRICAO) VALUES ('X')")
        finally:
            oracle.execute(f"REVOKE INSERT, DELETE ON {CONTRIBUINTES_TABLE} FROM NC_LEITURA")

    def test_sync_contribuintes_detects_a_session_that_is_not_read_only(self, oracle):
        config = oracle_leitura.read_config(environ=oracle.read_env())

        with oracledb.connect(
            user=config.user, password=config.secret, dsn=f"{config.host}:{config.port}/{config.service_name}"
        ) as connection:
            with pytest.raises(oracle_leitura.ReadOnlySessionError):
                oracle_leitura.assert_read_only(connection)

    def test_sync_contribuintes_never_changes_the_oracle_rows(self, oracle, sync):
        rows = [taxpayer(1), taxpayer(2)]
        oracle.replace_rows(rows)

        sync()

        with oracle.system() as connection, connection.cursor() as cursor:
            kept = cursor.execute(f"SELECT {PG_COLUMNS} FROM {CONTRIBUINTES_TABLE} ORDER BY INSCRICAO").fetchall()
        assert kept == rows

    @pytest.mark.parametrize("name", ["NFSE_RPE.MV_CONTRIBUINTES", "mv_contribuintes", "NFSE_RPE.MV_C$1"])
    def test_sync_contribuintes_accepts_plain_table_names(self, name):
        assert contribuintes.validate_table_name(name) == name.upper()

    @pytest.mark.parametrize("name", ["T; DROP TABLE X", "A.B.C", "T WHERE 1=1", "", '"T"'])
    def test_sync_contribuintes_refuses_a_table_name_that_is_not_an_identifier(self, name):
        with pytest.raises(ValueError, match="identificador"):
            contribuintes.validate_table_name(name)


@pytest.mark.usefixtures("prefect_api")
class TestSyncContribuintesFlow:
    def test_sync_contribuintes_flow_fails_before_connecting_without_the_oracle_variables(self, monkeypatch):
        monkeypatch.setenv("PG_NOTA_CARIOCA_SINCRONIZADOR__DSN", "postgresql://u@h/db")
        for key in ("HOST", "PORT", "SERVICE_NAME", "USERNAME", "PASSWORD"):
            monkeypatch.delenv(f"DB_ORACLE_NC_LEITURA__DB_{key}", raising=False)
        self._forbid_connections(monkeypatch)

        with pytest.raises(oracle_leitura.OracleLeituraConfigError, match="DB_ORACLE_NC_LEITURA__DB_HOST"):
            flow_contribuintes.rj_smfp__nota_carioca_contribuintes_postgres()

    def test_sync_contribuintes_flow_fails_before_connecting_without_the_postgres_dsn(self, monkeypatch):
        for key, value in {
            "DB_HOST": "oracle.test",
            "DB_PORT": "1521",
            "DB_SERVICE_NAME": "svc",
            "DB_USERNAME": "leitor",
            "DB_PASSWORD": "x",
        }.items():
            monkeypatch.setenv(f"DB_ORACLE_NC_LEITURA__{key}", value)
        monkeypatch.delenv("PG_NOTA_CARIOCA_SINCRONIZADOR__DSN", raising=False)
        self._forbid_connections(monkeypatch)

        with pytest.raises(postgres.PostgresConfigError, match="PG_NOTA_CARIOCA_SINCRONIZADOR__DSN"):
            flow_contribuintes.rj_smfp__nota_carioca_contribuintes_postgres()

    @staticmethod
    def _forbid_connections(monkeypatch: pytest.MonkeyPatch) -> None:
        def refuse(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("não podia conectar")

        monkeypatch.setattr(oracledb, "connect", refuse)
        monkeypatch.setattr(psycopg, "connect", refuse)

    def test_sync_contribuintes_flow_refuses_a_bad_table_name_before_connecting(self, monkeypatch):
        self._forbid_connections(monkeypatch)

        with pytest.raises(ValueError, match="identificador"):
            flow_contribuintes.rj_smfp__nota_carioca_contribuintes_postgres(oracle_table="T; DROP TABLE X")

    def test_sync_contribuintes_flow_syncs_an_insert_an_update_and_a_removal(self, database, oracle, monkeypatch):
        # Given: o flow de verdade apontado, só por variáveis de ambiente, para o Oracle e o Postgres de teste
        for key, value in oracle.read_env().items():
            monkeypatch.setenv(key, value)
        monkeypatch.setenv("PG_NOTA_CARIOCA_SINCRONIZADOR__DSN", database.sincronizador_dsn)
        oracle.replace_rows([taxpayer(1), taxpayer(2), taxpayer(3)])
        flow_contribuintes.rj_smfp__nota_carioca_contribuintes_postgres()

        # When: o Oracle ganha uma linha, altera outra e perde a terceira, e o flow roda de novo
        oracle.replace_rows([taxpayer(1), taxpayer(2, categoria=Decimal(7)), taxpayer(4)])
        flow_contribuintes.rj_smfp__nota_carioca_contribuintes_postgres()

        # Then: o Postgres tem a inserção, a alteração e sem a remoção
        assert pg_rows(database) == [taxpayer(1), taxpayer(2, categoria=Decimal(7)), taxpayer(4)]
