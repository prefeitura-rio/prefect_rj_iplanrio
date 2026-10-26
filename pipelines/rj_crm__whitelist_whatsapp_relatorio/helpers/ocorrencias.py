"""Normalização das linhas devolvidas pela query de URLs redigidas."""

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from pydantic import JsonValue, TypeAdapter, ValidationError
from toolkit.telemetry import logger

JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


@dataclass(frozen=True)
class Ocorrencia:
    """A step of the agent in which at least one URL was redacted."""

    id_passo: str
    datahora: datetime
    urls: tuple[str, ...]


def itens_validos(valores: Iterable[object]) -> tuple[str, ...]:
    """Return the clean URLs of a sequence, and drop the empty ones."""
    return tuple(str(item).strip() for item in valores if str(item).strip())


def extrair_urls(valor: object) -> tuple[str, ...]:
    """Return the content of `redacted_urls` as a tuple of URLs.

    The column has the `JSON` type of BigQuery, and it can arrive as a list, as a JSON
    string or as a scalar. Every format is accepted. The tuple may be empty.
    """
    if isinstance(valor, list | tuple):
        return itens_validos(valor)
    if valor is None:
        return ()

    texto = str(valor).strip()
    if not texto:
        return ()

    try:
        decodificado = JSON.validate_json(texto)
    except ValidationError:
        decodificado = texto

    if isinstance(decodificado, list):
        return itens_validos(decodificado)
    if decodificado is None:
        return ()
    return itens_validos([decodificado])


def normalizar(dados: list[dict[str, object]]) -> list[Ocorrencia]:
    """Return the result of the query as typed occurrences, in the order they came.

    A row with no usable URL is dropped and counted in the log. The query already
    filters `redacted_urls IS NOT NULL`, so such a row means an unexpected format.
    """
    ocorrencias: list[Ocorrencia] = []
    descartadas = 0
    for linha in dados:
        urls = extrair_urls(linha.get("redacted_urls"))
        inicio = linha["inicio_datahora"]
        if not urls or not isinstance(inicio, datetime):
            descartadas += 1
            continue
        ocorrencias.append(Ocorrencia(id_passo=str(linha["id_passo"]), datahora=inicio, urls=urls))

    if descartadas:
        logger.warning("%d linha(s) com redacted_urls em formato inesperado foram descartadas", descartadas)

    return ocorrencias


def ler_ultima_ocorrencia(dados: list[dict[str, object]]) -> datetime | None:
    """Return the time of the last known occurrence, or `None` if there never was one.

    `dados` is the result of `get_ultima_ocorrencia`, with the column
    `ultima_ocorrencia`.
    """
    if not dados:
        return None
    valor = dados[0]["ultima_ocorrencia"]
    return valor if isinstance(valor, datetime) else None


def descrever_ultima_ocorrencia(ultima: datetime | None, referencia: datetime) -> str:
    """Say how long ago the last occurrence known to the source was.

    The phrase goes into the Discord message on a day with no occurrence. It tells a
    quiet day from a source that stopped making data: the distance grows alone, and a
    reader of the channel sees it, with no threshold to set and no state to keep.

    `ultima` is the time of the last occurrence, or `None`. `referencia` is the date to
    measure from, the end of the window.
    """
    if ultima is None:
        return "Nenhuma ocorrência registrada na fonte até o momento."

    quando = ultima.strftime("%d/%m/%Y")
    dias = (referencia.date() - ultima.date()).days
    if dias <= 0:
        return f"Última ocorrência conhecida: {quando}."
    if dias == 1:
        return f"Última ocorrência conhecida: {quando} (há 1 dia)."
    return f"Última ocorrência conhecida: {quando} (há {dias} dias)."


def urls_distintas(ocorrencias: list[Ocorrencia]) -> list[str]:
    """List the unique URLs, in the order they first appear.

    They go to the copy block of the e-mail, because the whitelist takes the URL and not
    the occurrence.
    """
    vistas: dict[str, None] = {}
    for ocorrencia in ocorrencias:
        for url in ocorrencia.urls:
            vistas.setdefault(url, None)
    return list(vistas)


def frequencias(ocorrencias: list[Ocorrencia]) -> list[dict[str, object]]:
    """Count how many times each URL appeared, from the most frequent to the least.

    Return records with the keys `url` and `total`.
    """
    contagem = Counter(url for ocorrencia in ocorrencias for url in ocorrencia.urls)
    return [{"url": url, "total": total} for url, total in contagem.most_common()]


def para_template(ocorrencias: list[Ocorrencia]) -> list[dict[str, str]]:
    """Flatten the occurrences into one row for each URL, for the table of the e-mail.

    Return records with `datahora`, `url` and `id_passo`.
    """
    return [
        {
            "datahora": ocorrencia.datahora.strftime("%d/%m/%Y %H:%M"),
            "url": url,
            "id_passo": ocorrencia.id_passo,
        }
        for ocorrencia in ocorrencias
        for url in ocorrencia.urls
    ]
