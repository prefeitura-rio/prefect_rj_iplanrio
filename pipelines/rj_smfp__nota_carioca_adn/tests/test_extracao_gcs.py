"""Adaptador real do GCS contra o ``fake-gcs-server`` (testcontainers): precondições de geração de verdade."""

import socket
from collections.abc import Iterator

import pytest
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from pipelines.rj_smfp__nota_carioca_adn.utils.armazenamento import GcsArmazenamento, ObjetoJaExiste, gravar_imutavel

IMAGEM = "fsouza/fake-gcs-server:1.52"
PORTA_INTERNA = 4443
BUCKET = "bucket-de-teste"


def _porta_livre() -> int:
    with socket.socket() as soquete:
        soquete.bind(("127.0.0.1", 0))
        return soquete.getsockname()[1]


@pytest.fixture(scope="module")
def gcs() -> Iterator[GcsArmazenamento]:
    # O fake-gcs-server monta os links de download com a URL externa; ela precisa ser a que o teste alcança.
    porta = _porta_livre()
    endpoint = f"http://127.0.0.1:{porta}"
    container = DockerContainer(IMAGEM).with_command(
        f"-scheme http -port {PORTA_INTERNA} -backend memory -external-url {endpoint}"
    )
    container.ports[f"{PORTA_INTERNA}/tcp"] = ("127.0.0.1", porta)  # só em 127.0.0.1
    with container:
        wait_for_logs(container, "server started at", timeout=60)
        cliente = storage.Client(
            project="teste", credentials=AnonymousCredentials(), client_options={"api_endpoint": endpoint}
        )
        cliente.create_bucket(BUCKET)
        yield GcsArmazenamento(cliente, BUCKET)


def test_extracao_gcs_nunca_sobrescreve_um_objeto_existente(gcs: GcsArmazenamento) -> None:
    # Given: um objeto gravado
    gravar_imutavel(gcs, "adn/staging/dt=2026-01-05/lote_a.ndjson.gz", b"original")

    # When / Then: gravar outra vez o mesmo nome é recusado e o conteúdo original permanece
    with pytest.raises(ObjetoJaExiste):
        gravar_imutavel(gcs, "adn/staging/dt=2026-01-05/lote_a.ndjson.gz", b"outro conteudo")
    assert gcs.ler("adn/staging/dt=2026-01-05/lote_a.ndjson.gz") == b"original"
    assert gcs.uri("x/y") == f"gs://{BUCKET}/x/y"


def test_extracao_gcs_operacoes_de_trava_respeitam_a_geracao(gcs: GcsArmazenamento) -> None:
    # Given: um objeto de trava criado só se ausente
    geracao = gcs.criar_se_ausente("adn/staging/_trava_teste.json", b"v1")
    assert geracao is not None
    assert gcs.criar_se_ausente("adn/staging/_trava_teste.json", b"v1-de-outro") is None

    # When / Then: substituir com geração errada falha; com a certa troca e devolve a geração nova
    assert gcs.substituir_se_geracao("adn/staging/_trava_teste.json", b"v2", geracao + 1) is None
    nova = gcs.substituir_se_geracao("adn/staging/_trava_teste.json", b"v2", geracao)
    assert nova is not None
    assert nova != geracao
    assert gcs.ler_com_geracao("adn/staging/_trava_teste.json") == (b"v2", nova)

    # E: apagar com a geração atual apaga. (O fake-gcs-server 1.52 ignora ifGenerationMatch no DELETE, então a recusa
    # com geração errada não é verificável aqui; o fake em memória dos outros testes cobre essa regra da porta.)
    assert gcs.apagar_se_geracao("adn/staging/_trava_teste.json", nova) is True
    assert gcs.ler_com_geracao("adn/staging/_trava_teste.json") is None
