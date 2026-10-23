"""O dublê só vale se falar o contrato: as respostas dele são validadas contra o schema oficial do swagger."""

import json
import ssl
from datetime import datetime
from pathlib import Path

import httpx
import pytest

from pipelines.rj_smfp__nota_carioca_adn.tests.certificados import Pki
from pipelines.rj_smfp__nota_carioca_adn.tests.duble_adn import (
    Comportamento,
    DubleAdn,
    FimComo,
    erro_adn,
)

SCHEMAS = json.loads((Path(__file__).parent / "schema_lote_distribuicao.json").read_text())["schemas"]
TIPOS = {"string": str, "integer": int, "array": list, "object": dict}


def _validar(valor: object, esquema: dict[str, object], caminho: str) -> None:
    """Valida o subconjunto de OpenAPI 3.0 que o schema usa: $ref, allOf, enum, nullable, tipos e propriedades."""
    if valor is None:
        assert esquema.get("nullable"), f"{caminho}: nulo não permitido"
        return
    if "$ref" in esquema:
        _validar(valor, SCHEMAS[str(esquema["$ref"]).rsplit("/", 1)[-1]], caminho)
        return
    for parte in esquema.get("allOf", []):  # type: ignore[attr-defined]
        _validar(valor, parte, caminho)
    if "enum" in esquema:
        assert valor in esquema["enum"], f"{caminho}: {valor!r} fora do enum"  # type: ignore[operator]
    tipo = esquema.get("type")
    if tipo is None:
        return
    assert isinstance(valor, TIPOS[str(tipo)]), f"{caminho}: tipo {type(valor).__name__} em vez de {tipo}"
    if tipo == "string" and esquema.get("format") == "date-time":
        datetime.fromisoformat(str(valor))
    if tipo == "array":
        for indice, item in enumerate(valor):  # type: ignore[arg-type]
            _validar(item, esquema["items"], f"{caminho}[{indice}]")  # type: ignore[arg-type]
    if tipo == "object":
        propriedades: dict[str, dict[str, object]] = esquema.get("properties", {})  # type: ignore[assignment]
        extras = set(valor) - set(propriedades)  # type: ignore[call-overload]
        assert not extras or esquema.get("additionalProperties", True), f"{caminho}: propriedades fora do schema {extras}"
        for nome, subvalor in valor.items():  # type: ignore[attr-defined]
            _validar(subvalor, propriedades[nome], f"{caminho}.{nome}")


def _consultar(duble: DubleAdn, pki: Pki, caminho: str) -> httpx.Response:
    contexto = ssl.create_default_context(cafile=str(pki.ca_pem))
    contexto.load_cert_chain(pki.cliente_pem)
    with httpx.Client(verify=contexto) as http:
        return http.get(f"{duble.url}{caminho}")


@pytest.mark.parametrize("fim_como", ["404", "200_vazio", "200_status"])
def test_cliente_respostas_do_duble_seguem_o_schema_do_swagger(fim_como: FimComo, duble: DubleAdn, pki: Pki) -> None:
    # Given: lote cheio, lote final, fim de dados e rejeição, cada um como o dublê responde
    duble.comportamento = Comportamento(total_nsu=60, fim_como=fim_como)
    consultas = [
        "/municipios/DFe/0?tipoNSU=DISTRIBUICAO&lote=true",
        "/municipios/DFe/55?tipoNSU=DISTRIBUICAO&lote=true",
        "/municipios/DFe/4?tipoNSU=DISTRIBUICAO&lote=false",
        "/municipios/DFe/60?tipoNSU=DISTRIBUICAO&lote=true",
    ]

    # When / Then: o corpo de todas segue LoteDistribuicaoNSUResponse
    for consulta in consultas:
        resposta = _consultar(duble, pki, consulta)
        _validar(resposta.json(), SCHEMAS["LoteDistribuicaoNSUResponse"], consulta)
    duble.comportamento.roteiro.append(erro_adn(400, "E2000"))
    rejeicao = _consultar(duble, pki, consultas[0])
    _validar(rejeicao.json(), SCHEMAS["LoteDistribuicaoNSUResponse"], "rejeicao")
    assert rejeicao.status_code == 400


def test_cliente_item_do_duble_traz_todos_os_campos_do_schema_de_distribuicao(duble: DubleAdn, pki: Pki) -> None:
    # Given/When: um lote com NFS-e e evento
    corpo = _consultar(duble, pki, "/municipios/DFe/0?tipoNSU=DISTRIBUICAO&lote=true").json()

    # Then: cada item tem NSU, ChaveAcesso, TipoDocumento, ArquivoXml e DataHoraGeracao; o evento tem TipoEvento
    for item in corpo["LoteDFe"]:
        assert {"NSU", "ChaveAcesso", "TipoDocumento", "ArquivoXml", "DataHoraGeracao"} <= set(item)
        assert ("TipoEvento" in item) == (item["TipoDocumento"] == "EVENTO")
