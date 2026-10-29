"""Publicação A/B de ponta a ponta: o flow lê o export, carrega o slot inativo, publica e só então apaga o GCS."""

import gzip
import json
import re
from dataclasses import dataclass, field

import httpx
import psycopg
import pytest
from conftest import Database
from synthetic import (
    BUCKET,
    DATASET,
    DETAIL,
    RUN_ID,
    SNAPSHOT,
    MemoryObjectStore,
    detail_row,
    stage_export,
    summary_row,
)

from pipelines.rj_smfp__nota_carioca_bq_to_postgres import flow as flow_module
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import notify, object_store
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.discord import DiscordStatusMessage

pytestmark = pytest.mark.usefixtures("prefect_api")
WEBHOOK = "https://discord.test/api/webhooks/1/token"
PROD_DEPLOYMENT = "rj-smfp--nota-carioca-bq-to-postgres--prod"
STAGING_DEPLOYMENT = "rj-smfp--nota-carioca-bq-to-postgres--staging"


class AuditedStore(MemoryObjectStore):
    """GCS em memória que registra qual publicação estava ativa no banco quando os arquivos foram apagados."""

    def __init__(self, database: Database) -> None:
        super().__init__()
        self.database = database
        self.active_when_deleted: list[str] = []

    def delete(self, bucket: str, names) -> None:
        with self.database.admin() as admin:
            row = admin.execute("SELECT flow_run_id FROM public.publicacoes WHERE ativo").fetchone()
        self.active_when_deleted.append(row[0] if row else "")
        super().delete(bucket, names)


@dataclass
class World:
    """O flow de verdade, com o GCS em memória, o Postgres de teste e o webhook do Discord falso."""

    database: Database
    store: AuditedStore
    monkeypatch: pytest.MonkeyPatch
    requests: list[dict[str, object]] = field(default_factory=list)

    def __post_init__(self) -> None:
        transport = httpx.MockTransport(self._discord)
        self.monkeypatch.setattr(
            notify, "make_message", lambda _environment: DiscordStatusMessage(WEBHOOK, 0, transport)
        )
        self.monkeypatch.setattr(object_store, "open_store", lambda _project: self.store)
        self.monkeypatch.setattr(flow_module, "inject_bd_credentials_task", lambda **_kwargs: None)
        self.monkeypatch.setenv("PG_NOTA_CARIOCA__DSN", self.database.carregador_dsn)
        self.monkeypatch.setenv("PREFECT__RUNTIME__DEPLOYMENT__NAME", PROD_DEPLOYMENT)

    def _discord(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "42"})

    def stage(self, run_id: str = RUN_ID, details: int = 3, manifest_detail_rows: int | None = None) -> str:
        rows = None if manifest_detail_rows is None else {DETAIL: manifest_detail_rows}
        summaries = [summary_row(number) for number in (1, 2)]
        staged = stage_export(
            self.store, summaries, [detail_row(n) for n in range(1, details + 1)], run_id=run_id, manifest_rows=rows
        )
        return staged.uri

    def run(self, uri: str, **parameters: object) -> None:
        flow_module.rj_smfp__nota_carioca_bq_to_postgres(manifesto_uri=uri, **parameters)

    def query(self, statement: str) -> list[tuple]:
        with self.database.admin() as admin:
            return admin.execute(statement).fetchall()

    @property
    def announcements(self) -> list[str]:
        return [str(body["content"]) for body in self.requests if "content" in body]

    @property
    def embeds(self) -> list[dict[str, object]]:
        return [body["embeds"][0] for body in self.requests if "embeds" in body]


@pytest.fixture
def world(database: Database, monkeypatch: pytest.MonkeyPatch) -> World:
    return World(database, AuditedStore(database), monkeypatch)


def served_slot(world: World) -> str:
    definition = world.query(
        "SELECT definition FROM pg_views WHERE viewname = 'mvt_notas_nacionais_exigiveis_detalhes'"
    )
    return re.search(r"exigiveis_([ab])\.", definition[0][0]).group(1)


def test_two_tables_are_loaded_and_the_views_point_to_the_new_slot_with_the_right_flow_run_id(world):
    # Given: um export de 2 tabelas sintéticas e o banco servindo o slot A (estado inicial)
    uri = world.stage()
    assert served_slot(world) == "a"

    # When: roda o flow
    world.run(uri)

    # Then: as views servem o slot B, com as linhas carregadas, e a publicação ativa traz o flow run do manifesto
    assert served_slot(world) == "b"
    assert world.query("SELECT count(*) FROM public.mvt_notas_nacionais_exigiveis_detalhes") == [(3,)]
    assert world.query("SELECT count(*) FROM public.mvt_notas_nacionais_exigiveis") == [(2,)]
    slot, run_id, fonte, resumo, detalhe, checksum = world.query(
        "SELECT slot, flow_run_id, fonte, linhas_resumo, linhas_detalhe, checksum FROM public.publicacoes WHERE ativo"
    )[0]
    assert (slot, run_id, fonte, resumo, detalhe) == ("b", RUN_ID, f"bq_to_oracle:{DATASET}", 2, 3)
    assert re.fullmatch(r"[0-9a-f]{32}", checksum)


def test_the_publication_records_the_snapshot_of_the_manifest_as_the_replayer_expects(world):
    world.run(world.stage())

    assert world.query("SELECT snapshot_bq FROM public.publicacoes WHERE ativo") == [(SNAPSHOT,)]


def test_the_next_export_goes_to_the_other_slot_and_leaves_the_previous_one_untouched(world):
    world.run(world.stage("run-1", details=3))

    world.run(world.stage("run-2", details=1))

    assert served_slot(world) == "a"
    assert world.query("SELECT count(*) FROM public.mvt_notas_nacionais_exigiveis_detalhes") == [(1,)]
    assert world.query("SELECT count(*) FROM exigiveis_b.mvt_notas_nacionais_exigiveis_detalhes") == [(3,)]
    assert world.query("SELECT flow_run_id, ativo FROM public.publicacoes ORDER BY id") == [
        ("run-1", False),
        ("run-2", True),
    ]


def test_the_files_and_the_manifest_are_deleted_only_after_the_publication(world):
    uri = world.stage()
    exported = len(world.store.objects)

    world.run(uri)

    assert world.store.objects == {}
    assert len(world.store.deleted) == exported
    assert set(world.store.active_when_deleted) == {RUN_ID}


def test_a_csv_with_one_line_less_than_the_manifest_fails_the_publication_and_the_view_stays_on_the_previous_slot(
    world,
):
    # Given: uma publicação anterior no slot B e um novo export cujo manifesto promete 4 detalhes, mas o CSV traz 3
    world.run(world.stage("run-1"))
    uri = world.stage("run-2", details=3, manifest_detail_rows=4)
    before = world.query("SELECT definition FROM pg_views WHERE viewname = 'mvt_notas_nacionais_exigiveis_detalhes'")
    requests_before = len(world.requests)

    # When: o flow roda
    with pytest.raises(psycopg.errors.DataException, match="contagem divergente"):
        world.run(uri)

    # Then: a view continua no slot anterior, o histórico não ganha linha, os arquivos ficam e o Discord recebe o erro
    assert (
        world.query("SELECT definition FROM pg_views WHERE viewname = 'mvt_notas_nacionais_exigiveis_detalhes'")
        == before
    )
    assert world.query("SELECT flow_run_id FROM public.publicacoes WHERE ativo") == [("run-1",)]
    assert world.query("SELECT count(*) FROM public.publicacoes") == [(1,)]
    assert len(world.store.objects) > 0
    assert all(bucket == BUCKET for bucket, _ in world.store.objects)
    failure = world.announcements[-1]
    assert len(world.requests) > requests_before
    assert failure.startswith("❌")
    assert "contagem divergente" in failure


def test_a_load_that_fails_keeps_the_export_for_a_retry_and_reports_the_step(world):
    uri = world.stage()
    name = next(name for _, name in world.store.objects if name.endswith("part-000000.csv.gz") and DETAIL in name)
    world.store.objects[BUCKET, name] = b"isto nao e gzip"

    with pytest.raises(gzip.BadGzipFile):
        world.run(uri)

    assert (BUCKET, name) in world.store.objects
    assert world.store.deleted == []
    assert world.query("SELECT count(*) FROM public.publicacoes") == [(0,)]
    assert "Carga das tabelas" in world.announcements[-1]


def test_staging_posts_with_the_staging_mark(world, monkeypatch):
    monkeypatch.setenv("PREFECT__RUNTIME__DEPLOYMENT__NAME", STAGING_DEPLOYMENT)

    world.run(world.stage())

    assert world.embeds
    assert all(str(embed["title"]).startswith("🧪 STAGING · ") for embed in world.embeds)
    assert world.announcements[-1].startswith("🧪 STAGING · ")


def test_the_environment_parameter_overrides_the_one_from_the_deployment_name(world):
    world.run(world.stage(), ambiente="staging")

    assert all(str(embed["title"]).startswith("🧪 STAGING · ") for embed in world.embeds)


def test_discord_can_be_turned_off(world):
    world.run(world.stage(), discord_notifications=False)

    assert world.requests == []
    assert served_slot(world) == "b"


def test_an_export_without_the_detail_table_is_refused_before_touching_the_database(world):
    uri = world.stage()
    document = json.loads(world.store.read_text(BUCKET, f"{DATASET}/{RUN_ID}/manifesto.json"))
    del document["tabelas"][DETAIL]
    world.store.objects[BUCKET, f"{DATASET}/{RUN_ID}/manifesto.json"] = json.dumps(document).encode()

    with pytest.raises(ValueError, match=DETAIL):
        world.run(uri)

    assert world.query("SELECT count(*) FROM public.publicacoes") == [(0,)]
