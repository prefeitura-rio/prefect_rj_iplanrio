"""Exceções tipadas do cliente do ADN, incluindo os erros E2000-E2070 do Anexo IV do manual."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipelines.rj_smfp__nota_carioca_adn.utils.adn_decode import ItemAdn


class AdnErro(Exception):  # noqa: N818 - nomes em português, como AdnIndisponivel e DocumentoIlegivel do plano
    """Base de todas as falhas do cliente do ADN."""


class ConfiguracaoAdnInvalida(AdnErro, ValueError):  # noqa: N818 - idem
    """Variável de ambiente ausente ou PFX/senha inválidos."""


class AdnErroTransporte(AdnErro):
    """Falha de rede, de TLS ou de tempo limite antes de haver resposta HTTP."""


class AdnErroHttp(AdnErro):
    """Status HTTP inesperado, sem corpo de rejeição do ADN.

    :param status: Código HTTP.
    :param trecho: Início do corpo da resposta, para diagnóstico.
    """

    def __init__(self, status: int, trecho: str) -> None:
        super().__init__(f"ADN respondeu HTTP {status}: {trecho}")
        self.status = status
        self.trecho = trecho


class AdnIndisponivel(AdnErro):
    """O ADN limitou (429) ou ficou indisponível (503) além do tolerado.

    :param status: Último status recebido.
    :param pausa_s: Pausa do circuit breaker já cumprida antes de levantar; ``0`` quando só as tentativas acabaram.
    """

    def __init__(self, status: int, pausa_s: float) -> None:
        super().__init__(f"ADN indisponível (HTTP {status}); pausa de {pausa_s:.0f} s cumprida")
        self.status = status
        self.pausa_s = pausa_s


class ContratoAdnViolado(AdnErro):
    """A resposta contradiz a configuração (variante de cursor, tamanho do lote) ou o schema."""


class AdnRejeicao(AdnErro):
    """Rejeição do ADN com código de erro.

    :param codigo: Código do manual (``E2000``...).
    :param descricao: Descrição enviada pelo ADN.
    :param status: Status HTTP da resposta.
    """

    def __init__(self, codigo: str, descricao: str, status: int) -> None:
        super().__init__(f"ADN rejeitou com {codigo or 'sem código'} (HTTP {status}): {descricao}")
        self.codigo = codigo
        self.descricao = descricao
        self.status = status


class CertificadoRejeitado(AdnRejeicao):
    """E2000: erro no certificado de transmissão."""


class CnpjNaoCadastrado(AdnRejeicao):
    """E2001: CNPJ não cadastrado para nenhum município conveniado."""


class MunicipioSuspenso(AdnRejeicao):
    """E2003: o município existe, mas está suspenso no convênio."""


class MunicipioInativo(AdnRejeicao):
    """E2004: o município está inativo há mais do prazo no convênio."""


class NenhumDocumentoLocalizado(AdnRejeicao):
    """E2020, E2040 e E2070: não há documento a partir do NSU (ou para ele). Sinal de fim, não falha."""


class ChaveNaoLocalizada(AdnRejeicao):
    """E2060: não existe documento para a chave de acesso."""


class ChaveNaoReferenciaMunicipio(AdnRejeicao):
    """E2061: a chave de acesso não referencia o município solicitante."""


CODIGOS_ADN: dict[str, type[AdnRejeicao]] = {
    "E2000": CertificadoRejeitado,
    "E2001": CnpjNaoCadastrado,
    "E2003": MunicipioSuspenso,
    "E2004": MunicipioInativo,
    "E2020": NenhumDocumentoLocalizado,
    "E2040": NenhumDocumentoLocalizado,
    "E2060": ChaveNaoLocalizada,
    "E2061": ChaveNaoReferenciaMunicipio,
    "E2070": NenhumDocumentoLocalizado,
}


def rejeicao_do_codigo(codigo: str, descricao: str, status: int) -> AdnRejeicao:
    """Cria a exceção da classe do código; um código fora do Anexo IV vira ``AdnRejeicao`` genérica."""
    return CODIGOS_ADN.get(codigo, AdnRejeicao)(codigo, descricao, status)


class DocumentoIlegivel(AdnErro):
    """O ``ArquivoXml`` do item não pôde ser decodificado (base64, gzip, UTF-8) ou faltam XML ou data.

    :param item: Item como o ADN o enviou, para quem precisa preservá-lo.
    :param motivo: O que falhou.
    """

    def __init__(self, item: "ItemAdn", motivo: str) -> None:
        super().__init__(f"Documento ilegível no NSU {item.nsu}: {motivo}")
        self.item = item
        self.nsu = item.nsu
        self.motivo = motivo
