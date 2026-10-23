"""CLI ``sondar``: só metadados, nunca o XML, e só com ``--dry-run``."""

import json

import pytest

from pipelines.rj_smfp__nota_carioca_adn import cli
from pipelines.rj_smfp__nota_carioca_adn.tests import xml_sintetico
from pipelines.rj_smfp__nota_carioca_adn.tests.certificados import Pki
from pipelines.rj_smfp__nota_carioca_adn.tests.duble_adn import Comportamento, DubleAdn


@pytest.fixture
def ambiente_adn(monkeypatch: pytest.MonkeyPatch, duble: DubleAdn, pki: Pki) -> None:
    monkeypatch.setenv("ADN_PFX_BASE64", pki.pfx_base64)
    monkeypatch.setenv("ADN_PFX_SENHA", pki.senha)
    monkeypatch.setenv("ADN_BASE_URL", duble.url)
    monkeypatch.setenv("ADN_CA_BUNDLE", str(pki.ca_pem))


@pytest.mark.usefixtures("ambiente_adn")
def test_cliente_sondar_imprime_so_metadados_sem_o_xml(
    duble: DubleAdn, capsys: pytest.CaptureFixture[str]
) -> None:
    # Given: o dublê com cursor exclusivo
    # When: sonda o NSU 100 em dry-run
    codigo = cli.main(["sondar", "--nsu", "100", "--dry-run"])

    # Then: exit 0, um JSON com a janela observada e a variante de cursor, e nenhum trecho do XML ou do PFX
    saida = capsys.readouterr().out
    relatorio = json.loads(saida)
    assert codigo == 0
    assert relatorio["lote"]["primeiro_nsu"] == 101
    assert relatorio["lote"]["ultimo_nsu"] == 150
    assert relatorio["lote"]["quantidade"] == 50
    assert relatorio["lote"]["cursor_observado"] == "exclusivo"
    amostra = relatorio["lote"]["amostra"][0]
    assert amostra["chave_acesso"] == xml_sintetico.chave_acesso(101)
    assert amostra["tamanho_xml_bytes"] == len(xml_sintetico.documento_xml(101).encode())
    assert "<" not in saida
    assert "xml" not in {chave.lower() for chave in amostra}
    assert [requisicao.lote for requisicao in duble.requisicoes] == [True, False]


@pytest.mark.usefixtures("ambiente_adn")
def test_cliente_sondar_detecta_cursor_inclusivo(duble: DubleAdn, capsys: pytest.CaptureFixture[str]) -> None:
    # Given: o dublê na variante inclusiva e de lote 100
    duble.comportamento = Comportamento(cursor_inclusivo=True, lote_max=100)

    # When
    cli.main(["sondar", "--nsu", "100", "--dry-run"])

    # Then: o primeiro NSU devolvido é o próprio 100 e a variante aparece como inclusiva, com 100 itens
    lote = json.loads(capsys.readouterr().out)["lote"]
    assert (lote["primeiro_nsu"], lote["cursor_observado"], lote["quantidade"]) == (100, "inclusivo", 100)


@pytest.mark.usefixtures("ambiente_adn")
def test_cliente_sondar_exige_dry_run_e_nao_chama_o_adn_sem_ele(
    duble: DubleAdn, capsys: pytest.CaptureFixture[str]
) -> None:
    # When / Then: sem --dry-run o parser recusa (exit 2) e o ADN não recebe nada
    with pytest.raises(SystemExit) as saida:
        cli.main(["sondar", "--nsu", "100"])
    assert saida.value.code == 2
    assert duble.requisicoes == []
    assert "dry-run" in capsys.readouterr().err
