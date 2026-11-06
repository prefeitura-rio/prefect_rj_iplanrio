"""Erros de documento: tudo o que leva um XML para a quarentena em vez de para as tabelas raw.

Os nomes são os do domínio (``VersaoNaoSuportada``...), por isso a exceção ao sufixo ``Error`` (N818).
"""

from collections.abc import Sequence


class ErroDocumento(Exception):  # noqa: N818
    """Falha permanente de um documento (nunca vale tentar de novo com o mesmo parser).

    Espelha ``NonRetryableProcessingException`` do .NET: o documento vai para a quarentena com a mensagem como ``erro``.
    """


class XmlVazio(ErroDocumento):
    """O XML está nulo, vazio ou só tem espaços."""

    def __init__(self) -> None:
        super().__init__("Arquivo XML não encontrado ou vazio.")


class XmlMalformado(ErroDocumento):
    """O XML não é bem formado (não dá para ler o elemento raiz)."""

    def __init__(self, detalhe: str) -> None:
        super().__init__(f"XML malformado: {detalhe}")


class VersaoIlegivel(ErroDocumento):
    """O atributo ``versao`` do elemento raiz falta, está vazio ou não é uma versão."""

    def __init__(self, valor: str | None) -> None:
        super().__init__(f"Falha ao ler ou interpretar o atributo 'versao' do XML. Valor encontrado: '{valor}'.")


class VersaoNaoSuportada(ErroDocumento):
    """A versão declarada é maior que a máxima suportada pelo parser (1.01)."""

    def __init__(self, declarada: str, maxima: str) -> None:
        self.declarada = declarada
        super().__init__(
            f"Versão do schema do documento ({declarada}) não suportada. A versão máxima suportada é {maxima}."
        )


class XsdInexistente(ErroDocumento):
    """Não há XSD vendorizado para a versão declarada."""

    def __init__(self, versao: str) -> None:
        super().__init__(f"Não há esquema XSD para a versão {versao}.")


class TipoDocumentoDesconhecido(ErroDocumento):
    """O tipo do documento não é NFSE, EVENTO nem CNC."""

    def __init__(self, valor: object) -> None:
        super().__init__(f"Tipo de documento desconhecido: {valor!r}")


class XsdInvalido(ErroDocumento):
    """O XML tem erros de esquema fora das tolerâncias; ``erros`` lista os fatais."""

    def __init__(self, erros: Sequence[object]) -> None:
        self.erros = tuple(erros)
        super().__init__("Erro de validação de schema: " + "; ".join(str(e) for e in self.erros))
