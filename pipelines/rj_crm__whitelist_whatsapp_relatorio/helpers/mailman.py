"""Relatório detalhado por e-mail, via endpoint `/data/mailman` do Data Relay.

O corpo é o `email_template.html` renderizado com Jinja2, seguindo o padrão de
`rj_pic__disparos_email` (decisões D20 e D21).
"""

from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path

from httpx2 import post
from jinja2 import Environment, FileSystemLoader
from pydantic import JsonValue, TypeAdapter
from toolkit.telemetry import logger

from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.janela import Janela
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.ocorrencias import (
    Ocorrencia,
    frequencias,
    para_template,
    urls_distintas,
)
from utils.records import dig

JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)

DIRETORIO_TEMPLATE = Path(__file__).resolve().parent.parent
NOME_TEMPLATE = "email_template.html"


@dataclass(frozen=True)
class ConexaoDataRelay:
    """Endereço e credencial do endpoint `/data/mailman`."""

    url: str
    api_key: str
    timeout: int = 30


def montar_assunto(janela: Janela, ocorrencias: list[Ocorrencia]) -> str:
    """Build the subject of the e-mail, with the date and the volume.

    The inbox then shows the essentials.
    """
    return f"[Relatório diário] URLs redigidas — {janela.data_relatorio} ({len(ocorrencias)} ocorrências)"


def renderizar(janela: Janela, ocorrencias: list[Ocorrencia], run_url: str) -> str:
    """Render the HTML body of the report.

    `run_url` is the address of the run in the Prefect UI. Raise
    `jinja2.TemplateNotFound` if `email_template.html` is not in the pipeline.
    """
    ambiente = Environment(
        loader=FileSystemLoader(str(DIRETORIO_TEMPLATE)),
        autoescape=True,
        keep_trailing_newline=True,
    )
    distintas = urls_distintas(ocorrencias)
    return ambiente.get_template(NOME_TEMPLATE).render(
        data_relatorio=janela.data_relatorio,
        total_ocorrencias=len(ocorrencias),
        total_urls=len(distintas),
        janela_inicio=janela.inicio_exibicao,
        janela_fim=janela.fim_exibicao,
        urls=distintas,
        frequencias=frequencias(ocorrencias),
        ocorrencias=para_template(ocorrencias),
        run_url=run_url,
        tem_anexo=False,
    )


def enviar_relatorio(
    conexao: ConexaoDataRelay,
    destinatarios: list[str],
    janela: Janela,
    ocorrencias: list[Ocorrencia],
    run_url: str,
) -> None:
    """Build and send the detailed report of the period.

    It puts the subject, the body and the send in one call, so the task does not need to
    know how the message is composed. `destinatarios` becomes `to_addresses`. Raise
    `ValueError` if the list of recipients is empty or the send fails.
    """
    enviar(
        conexao=conexao,
        destinatarios=destinatarios,
        assunto=montar_assunto(janela=janela, ocorrencias=ocorrencias),
        corpo=renderizar(janela=janela, ocorrencias=ocorrencias, run_url=run_url),
    )


def enviar(conexao: ConexaoDataRelay, destinatarios: list[str], assunto: str, corpo: str) -> None:
    """Send the report through the Data Relay.

    `is_html_body` must be `True`: the endpoint default is `False`, and without it the
    HTML would arrive as raw text. Raise `ValueError` if the list of recipients is empty
    or the send fails.
    """
    if not destinatarios:
        raise ValueError("Nenhum destinatário configurado para o relatório.")

    resposta = post(
        conexao.url,
        headers={"x-api-key": conexao.api_key},
        json={
            "to_addresses": destinatarios,
            "subject": assunto,
            "body": corpo,
            "is_html_body": True,
        },
        timeout=conexao.timeout,
    )
    if resposta.status_code != HTTPStatus.OK:
        raise ValueError(f"Falha ao enviar o e-mail: {resposta.status_code} - {resposta.text}")

    conteudo = JSON.validate_json(resposta.content)
    if not dig(conteudo, "success"):
        raise ValueError(f"Data Relay recusou o envio: {dig(conteudo, 'message')}")
    logger.info("Relatório enviado para %d destinatário(s)", len(destinatarios))
