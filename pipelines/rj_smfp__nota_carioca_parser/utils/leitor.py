"""Leitura do XML validado com a semântica do ``XmlSerializer`` do .NET e do Oracle.

No .NET o XML vira um grafo de objetos (``XmlNotaNacional.vb``): elemento ausente dá ``Nothing`` em texto e em tipo
anulável, e ``0`` em ``Byte``/``Long``/``Double`` não anuláveis. O Oracle guarda ``''`` como ``NULL``. Estes
conversores reproduzem as duas coisas para que as colunas raw tenham o mesmo valor das do Oracle.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from lxml import etree

from pipelines.rj_smfp__nota_carioca_parser.constants import FUSO_ORACLE
from pipelines.rj_smfp__nota_carioca_parser.utils.arvore import filho, filhos
from pipelines.rj_smfp__nota_carioca_parser.utils.erros import ErroDocumento


class ErroExtracao(ErroDocumento):
    """Um valor do XML (aceito por tolerância no XSD) não converte para o tipo da coluna; o .NET também falharia."""


@dataclass(frozen=True, slots=True)
class No:
    """Elemento do XML navegável por nome local; nó ausente propaga ``None`` em vez de falhar (``?.`` do VB)."""

    elemento: etree._Element | None

    def __call__(self, *nomes: str) -> "No":
        atual = self.elemento
        for nome in nomes:
            atual = filho(atual, nome)
        return No(atual)

    def __bool__(self) -> bool:
        return self.elemento is not None

    def lista(self, nome: str) -> list["No"]:
        """Todos os filhos diretos com o nome."""
        return [No(f) for f in filhos(self.elemento, nome)]

    def atributo(self, nome: str) -> str | None:
        return None if self.elemento is None else self.elemento.get(nome)

    @property
    def texto(self) -> str | None:
        """Texto do elemento (``''`` se vazio); ``None`` se o elemento não existe."""
        return None if self.elemento is None else "".join(self.elemento.itertext())


def texto(no: No) -> str | None:
    """``String`` do .NET que o Oracle grava: vazio vira ``NULL``."""
    valor = no.texto
    return valor if valor else None


def aparado(no: No) -> str | None:
    """``If(String.IsNullOrWhiteSpace(x), Nothing, x.Trim())``: o padrão dos campos de pessoa."""
    valor = no.texto
    return None if valor is None or not valor.strip() else valor.strip()


def _numero(no: No) -> str | None:
    valor = no.texto
    return None if valor is None else valor.strip()


def inteiro_ou_zero(no: No, nome: str = "") -> int:
    """``Byte``/``Integer``/``Long`` não anulável: ausente é 0."""
    valor = _numero(no)
    if valor is None:
        return 0
    try:
        return int(valor)
    except ValueError as erro:
        raise ErroExtracao(f"Valor inteiro inválido em '{nome}': {valor!r}") from erro


def inteiro(no: No, nome: str = "") -> int | None:
    """Tipo anulável (``Byte?``, ``Long?``): ausente ou vazio é ``None``."""
    valor = _numero(no)
    if not valor:
        return None
    try:
        return int(valor)
    except ValueError as erro:
        raise ErroExtracao(f"Valor inteiro inválido em '{nome}': {valor!r}") from erro


def decimal_ou_zero(no: No, escala: int, nome: str = "") -> Decimal:
    """``Double`` não anulável gravado em ``NUMBER(p,escala)``: ausente é 0 e o Oracle arredonda na escala."""
    valor = _numero(no)
    if valor is None:
        return Decimal(0).quantize(Decimal(1).scaleb(-escala))
    try:
        numero = Decimal(valor)
    except InvalidOperation as erro:
        raise ErroExtracao(f"Valor numérico inválido em '{nome}': {valor!r}") from erro
    if not numero.is_finite():
        raise ErroExtracao(f"Valor numérico inválido em '{nome}': {valor!r}")
    return numero.quantize(Decimal(1).scaleb(-escala), rounding=ROUND_HALF_UP)


def data_hora(no: No, nome: str = "") -> datetime | None:
    """``DateTime`` do XmlSerializer gravado em ``DATE``: com deslocamento vira hora de Brasília; sem, fica como está.

    O .NET converte para a hora local do servidor (``RoundtripKind``); ``Z`` mantém a hora UTC sem conversão.
    """
    valor = _numero(no)
    if not valor:
        return None
    try:
        lido = datetime.fromisoformat(valor)
    except ValueError as erro:
        raise ErroExtracao(f"Data inválida em '{nome}': {valor!r}") from erro
    if lido.tzinfo is None or valor.endswith("Z"):
        return lido.replace(tzinfo=None, microsecond=0)
    return lido.astimezone(ZoneInfo(FUSO_ORACLE)).replace(tzinfo=None, microsecond=0)


def iso(valor: datetime | None) -> str | None:
    """Texto ``YYYY-MM-DDTHH:MM:SS`` (``DATETIME`` do BigQuery)."""
    return None if valor is None else valor.isoformat(timespec="seconds")


def preenchido_a_esquerda(valor: int, tamanho: int) -> str:
    """``LPAD(TO_CHAR(n), tamanho, '0')`` do Oracle, que trunca à direita se passar do tamanho."""
    return str(valor).rjust(tamanho, "0")[:tamanho]


__all__ = [
    "ErroExtracao",
    "No",
    "aparado",
    "data_hora",
    "decimal_ou_zero",
    "inteiro",
    "inteiro_ou_zero",
    "iso",
    "preenchido_a_esquerda",
    "texto",
]
