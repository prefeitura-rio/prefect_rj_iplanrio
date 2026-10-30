"""Adaptadores do GCS e do BigQuery contra clientes de mentira: sem rede, sem GCP."""

import gzip
import json
from types import SimpleNamespace

import pytest
from google.api_core.exceptions import Conflict, GoogleAPICallError

from pipelines.rj_smfp__nota_carioca_parser.utils.bigquery import ArmazemBigQuery, identificador_do_job
from pipelines.rj_smfp__nota_carioca_parser.utils.gcs import LeitorGcs, indexar_por_nsu, separar_uri


def ndjson(*linhas: dict) -> bytes:
    return "".join(json.dumps(linha) + "\n" for linha in linhas).encode()


def test_gcs_indexa_por_nsu_objeto_comprimido_ou_nao():
    conteudo = ndjson({"nsu": 5, "xml": "<a/>"}, {"nsu": 6, "xml": "<b/>"})
    assert set(indexar_por_nsu(gzip.compress(conteudo))) == {5, 6}
    assert indexar_por_nsu(conteudo)[6]["xml"] == "<b/>"
    assert indexar_por_nsu(gzip.compress(conteudo) + gzip.compress(ndjson({"nsu": 7})))[7]["nsu"] == 7


@pytest.mark.parametrize("uri", ["http://x/y", "gs://", "gs://bucket", "gs://bucket/", "bucket/obj"])
def test_gcs_uri_invalida_e_recusada(uri):
    with pytest.raises(ValueError, match="URI do GCS inválida"):
        separar_uri(uri)


def test_gcs_leitor_baixa_o_objeto_certo():
    pedidos = []
    blob = SimpleNamespace(download_as_bytes=lambda: gzip.compress(ndjson({"nsu": 9, "xml": "<x/>"})))
    bucket = SimpleNamespace(blob=lambda nome: pedidos.append(nome) or blob)
    cliente = SimpleNamespace(bucket=lambda nome: pedidos.append(nome) or bucket)
    assert LeitorGcs(cliente).ler_linhas("gs://rj-bucket/adn/prod/lote_1.ndjson.gz")[9]["xml"] == "<x/>"
    assert pedidos == ["rj-bucket", "adn/prod/lote_1.ndjson.gz"]


def test_bigquery_id_do_job_e_deterministico_e_so_tem_caracteres_validos():
    primeiro = identificador_do_job("nfse", "2026.10.1-1-9-abc")
    assert primeiro == identificador_do_job("nfse", "2026.10.1-1-9-abc")
    assert primeiro == "parser-nfse-2026_10_1-1-9-abc"
    assert identificador_do_job("nfse", "x", 2) == "parser-nfse-x-r2"


class JobFeito:
    def __init__(self, erro: Exception | None = None) -> None:
        self.erro = erro

    def result(self) -> None:
        if self.erro:
            raise self.erro


class ClienteBq:
    """Cliente de mentira: ids de job que já existem dão Conflict, como na API."""

    def __init__(self, existentes: dict[str, Exception | None] | None = None) -> None:
        self.jobs = dict(existentes or {})
        self.enviados: list[tuple[str, str, bytes]] = []

    def load_table_from_file(self, arquivo, destino, job_id, job_config, rewind):
        assert rewind and job_config.write_disposition == "WRITE_APPEND"
        if job_id in self.jobs:
            raise Conflict("já existe")
        self.jobs[job_id] = None
        self.enviados.append((job_id, destino, arquivo.read()))
        return JobFeito()

    def get_job(self, job_id):
        return JobFeito(self.jobs[job_id])


def armazem(cliente: ClienteBq) -> ArmazemBigQuery:
    return ArmazemBigQuery(cliente, "proj", "ds", "prod")


def test_bigquery_carga_envia_ndjson_com_job_id_deterministico_e_nao_repete_o_lote():
    cliente = ClienteBq()
    linhas = [{"nsu": 1, "prioridade": None, "erro": None, "versao_parser": "v", "avaliado_em": "2026-10-23T12:00:00+00:00"}]
    armazem(cliente).carregar("quarentena", "lote-1", linhas)
    armazem(cliente).carregar("quarentena", "lote-1", linhas)
    [(job_id, destino, corpo)] = cliente.enviados
    assert (job_id, destino) == ("parser-quarentena-lote-1", "proj.ds.quarentena")
    assert json.loads(corpo)["nsu"] == 1


def test_bigquery_job_anterior_que_falhou_gasta_o_id_e_a_nova_tentativa_usa_outro():
    cliente = ClienteBq({"parser-quarentena-lote-2": GoogleAPICallError("falhou")})
    armazem(cliente).carregar("quarentena", "lote-2", [{"nsu": 1, "versao_parser": "v", "avaliado_em": "2026-10-23T12:00:00Z"}])
    assert [job for job, _, _ in cliente.enviados] == ["parser-quarentena-lote-2-r1"]


def test_bigquery_falha_do_job_novo_sobe_para_quem_chamou():
    class Falha(ClienteBq):
        def load_table_from_file(self, *args, **kwargs):
            return JobFeito(GoogleAPICallError("load falhou"))

    with pytest.raises(GoogleAPICallError):
        armazem(Falha()).carregar("quarentena", "lote-3", [{"nsu": 1}])


def test_bigquery_marca_e_gravada_por_load_job_idempotente():
    cliente = ClienteBq()
    armazem(cliente).gravar_marca("v1", 500)
    armazem(cliente).gravar_marca("v1", 500)
    assert [(job, json.loads(corpo)) for job, _, corpo in cliente.enviados] == [
        ("parser-parser_marca-v1-500", {"versao_parser": "v1", "ultimo_nsu": 500})
    ]
