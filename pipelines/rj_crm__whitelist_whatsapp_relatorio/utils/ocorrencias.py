"""Normalização das linhas devolvidas pela query de URLs redigidas."""

import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from pipelines.rj_crm__whitelist_whatsapp_relatorio.utils.log import logger_da_pipeline

logger = logger_da_pipeline(__name__)


@dataclass(frozen=True)
class Ocorrencia:
    """Um passo do agente em que ao menos uma URL foi redigida."""

    id_passo: str
    datahora: datetime
    urls: tuple[str, ...]


def itens_validos(valores: object) -> tuple[str, ...]:
    """Converte uma sequência em URLs limpas, descartando vazios.

    :param valores: Sequência de valores brutos.
    :returns: URLs sem espaços em volta.
    """
    return tuple(str(item).strip() for item in valores if str(item).strip())


def extrair_urls(valor: object) -> tuple[str, ...]:
    """Normaliza o conteúdo de ``redacted_urls`` para uma tupla de URLs.

    A coluna vem do tipo ``JSON`` do BigQuery e pode chegar como lista, como string JSON
    ou como escalar. Todos os formatos são aceitos.

    :param valor: Valor bruto da coluna ``redacted_urls``.
    :returns: URLs encontradas, possivelmente vazia.
    """
    if isinstance(valor, (list, tuple)):
        return itens_validos(valor)
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return ()

    texto = str(valor).strip()
    if not texto:
        return ()

    try:
        decodificado = json.loads(texto)
    except json.JSONDecodeError:
        decodificado = texto

    if isinstance(decodificado, (list, tuple)):
        return itens_validos(decodificado)
    if decodificado is None:
        return ()
    return itens_validos([decodificado])


def normalizar(dados: pd.DataFrame) -> list[Ocorrencia]:
    """Converte o resultado da query em ocorrências tipadas.

    Linhas sem nenhuma URL utilizável são descartadas e contabilizadas no log: a query
    já filtra ``redacted_urls IS NOT NULL``, então elas indicam formato inesperado.

    :param dados: Resultado da query, com ``id_passo``, ``inicio_datahora`` e
        ``redacted_urls``.
    :returns: Ocorrências na ordem em que vieram.
    """
    if dados.empty:
        return []

    ocorrencias: list[Ocorrencia] = []
    descartadas = 0
    for linha in dados.to_dict("records"):
        urls = extrair_urls(linha.get("redacted_urls"))
        if not urls:
            descartadas += 1
            continue
        ocorrencias.append(
            Ocorrencia(
                id_passo=str(linha["id_passo"]),
                datahora=pd.to_datetime(linha["inicio_datahora"]).to_pydatetime(),
                urls=urls,
            )
        )

    if descartadas:
        logger.warning("%d linha(s) com redacted_urls em formato inesperado foram descartadas", descartadas)

    return ocorrencias


def ler_ultima_ocorrencia(dados: pd.DataFrame) -> datetime | None:
    """Extrai o instante da última ocorrência conhecida do resultado da query.

    :param dados: Resultado de ``get_ultima_ocorrencia``, com a coluna
        ``ultima_ocorrencia``.
    :returns: Instante da última ocorrência, ou ``None`` se a fonte nunca registrou uma.
    """
    if dados.empty:
        return None
    valor = dados["ultima_ocorrencia"].iloc[0]
    if pd.isna(valor):
        return None
    return pd.to_datetime(valor).to_pydatetime()


def descrever_ultima_ocorrencia(ultima: datetime | None, referencia: datetime) -> str:
    """Descreve há quanto tempo foi a última ocorrência conhecida na fonte.

    A frase entra na mensagem do Discord em dia sem ocorrências, e é o que distingue um
    dia tranquilo de uma fonte que parou de gerar dados: a distância cresce sozinha e
    fica visível para quem acompanha o canal, sem limiar configurado nem estado guardado.

    :param ultima: Instante da última ocorrência, ou ``None``.
    :param referencia: Data contra a qual medir a distância — o fim da janela.
    :returns: Frase pronta para a mensagem.
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
    """Lista as URLs únicas, preservando a ordem de aparição.

    É o que vai para o bloco de cópia do e-mail: o que se libera na whitelist é a URL,
    não a ocorrência.

    :param ocorrencias: Ocorrências do período.
    :returns: URLs sem repetição.
    """
    vistas: dict[str, None] = {}
    for ocorrencia in ocorrencias:
        for url in ocorrencia.urls:
            vistas.setdefault(url, None)
    return list(vistas)


def frequencias(ocorrencias: list[Ocorrencia]) -> list[dict[str, object]]:
    """Conta quantas vezes cada URL apareceu, da mais frequente para a menos.

    :param ocorrencias: Ocorrências do período.
    :returns: Registros com as chaves ``url`` e ``total``.
    """
    contagem = Counter(url for ocorrencia in ocorrencias for url in ocorrencia.urls)
    return [{"url": url, "total": total} for url, total in contagem.most_common()]


def para_template(ocorrencias: list[Ocorrencia]) -> list[dict[str, str]]:
    """Achata as ocorrências em uma linha por URL, para a tabela do e-mail.

    :param ocorrencias: Ocorrências do período.
    :returns: Registros com ``datahora``, ``url`` e ``id_passo``.
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
