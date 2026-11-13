"""CLI do pacote: ``python -m pipelines.rj_smfp__nota_carioca_adn.cli sondar --nsu <n> --dry-run``.

``sondar`` faz duas consultas ao ADN (``lote=true`` e ``lote=false``) com o NSU exatamente como informado e imprime só
metadados em JSON: janela de NSUs devolvida, quantidade, variante de cursor observada (inclusivo se o primeiro NSU do
lote é o próprio NSU consultado) e uma amostra com chave, tipos, data e tamanho do XML. O XML nunca é impresso nem
gravado, e o certificado vem das variáveis ``ADN_PFX_BASE64``, ``ADN_PFX_SENHA`` e ``ADN_BASE_URL``.
"""

import argparse
import json
import sys
from collections.abc import Sequence

from pipelines.rj_smfp__nota_carioca_adn.utils.adn_client import AdnClient, AdnConfig
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_decode import ItemAdn
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_errors import AdnErro, DocumentoIlegivel

TAMANHO_AMOSTRA = 5


def _metadados(item: ItemAdn) -> dict[str, object]:
    try:
        tamanho: int | None = len(item.decodificar().xml.encode())
        ilegivel = False
    except DocumentoIlegivel:
        tamanho, ilegivel = None, True
    return {
        "nsu": item.nsu,
        "chave_acesso": item.chave_acesso,
        "tipo_documento": item.tipo_documento,
        "tipo_evento": item.tipo_evento,
        "data_hora_geracao": item.data_hora_geracao.isoformat() if item.data_hora_geracao else None,
        "tamanho_xml_bytes": tamanho,
        "ilegivel": ilegivel,
    }


def _resumo(itens: tuple[ItemAdn, ...], nsu_consultado: int) -> dict[str, object]:
    if not itens:
        return {"quantidade": 0, "primeiro_nsu": None, "ultimo_nsu": None, "cursor_observado": None, "amostra": []}
    return {
        "quantidade": len(itens),
        "primeiro_nsu": itens[0].nsu,
        "ultimo_nsu": itens[-1].nsu,
        "cursor_observado": "inclusivo" if itens[0].nsu == nsu_consultado else "exclusivo",
        "amostra": [_metadados(item) for item in itens[:TAMANHO_AMOSTRA]],
    }


def sondar(nsu: int) -> dict[str, object]:
    """Consulta o ADN (lote e documento isolado) e devolve só os metadados."""
    with AdnClient(AdnConfig.do_ambiente()) as adn:
        return {
            "nsu_consultado": nsu,
            "lote": _resumo(adn.consultar(nsu, lote=True), nsu),
            "um": _resumo(adn.consultar(nsu, lote=False), nsu),
        }


def main(argv: Sequence[str] | None = None) -> int:
    """Ponto de entrada; devolve o código de saída."""
    parser = argparse.ArgumentParser(prog="rj_smfp__nota_carioca_adn")
    subcomandos = parser.add_subparsers(dest="comando", required=True)
    sonda = subcomandos.add_parser("sondar", help="Consulta um NSU no ADN e imprime só os metadados.")
    sonda.add_argument("--nsu", type=int, required=True, help="NSU a consultar, usado como está no caminho.")
    sonda.add_argument("--dry-run", action="store_true", help="Obrigatório: nada é gravado e o XML não é impresso.")
    argumentos = parser.parse_args(argv)
    if not argumentos.dry_run:
        parser.error("sondar só roda com --dry-run")
    try:
        relatorio = sondar(argumentos.nsu)
    except AdnErro as erro:
        print(f"erro: {type(erro).__name__}: {erro}", file=sys.stderr)
        return 1
    print(json.dumps(relatorio, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
