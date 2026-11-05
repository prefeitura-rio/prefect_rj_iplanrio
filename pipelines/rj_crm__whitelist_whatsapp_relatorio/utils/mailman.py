"""Relatório detalhado por e-mail, via endpoint `/data/mailman` do Data Relay.

O corpo é o `email_template.html` renderizado com Jinja2, seguindo o padrão de
`rj_pic__disparos_email` (decisões D20 e D21).
"""

from dataclasses import dataclass
from pathlib import Path

import requests
from jinja2 import Environment, FileSystemLoader

from pipelines.rj_crm__whitelist_whatsapp_relatorio.utils.janela import Janela
from pipelines.rj_crm__whitelist_whatsapp_relatorio.utils.log import logger_da_pipeline
from pipelines.rj_crm__whitelist_whatsapp_relatorio.utils.ocorrencias import (
    Ocorrencia,
    frequencias,
    para_template,
    urls_distintas,
)

logger = logger_da_pipeline(__name__)

DIRETORIO_TEMPLATE = Path(__file__).resolve().parent.parent
NOME_TEMPLATE = "email_template.html"


@dataclass(frozen=True)
class ConexaoDataRelay:
    """Endereço e credencial do endpoint `/data/mailman`."""

    url: str
    api_key: str
    timeout: int = 30


def montar_assunto(janela: Janela, ocorrencias: list[Ocorrencia]) -> str:
    """Monta o assunto do e-mail.

    :param janela: Recorte do relatório.
    :param ocorrencias: Ocorrências do período.
    :returns: Assunto com data e volume, para a caixa de entrada já dizer o essencial.
    """
    return f"[Relatório diário] URLs redigidas — {janela.data_relatorio} ({len(ocorrencias)} ocorrências)"


def renderizar(janela: Janela, ocorrencias: list[Ocorrencia], run_url: str) -> str:
    """Renderiza o corpo HTML do relatório.

    :param janela: Recorte do relatório.
    :param ocorrencias: Ocorrências do período.
    :param run_url: Endereço da execução na interface do Prefect.
    :returns: HTML pronto para envio.
    :raises jinja2.TemplateNotFound: Se ``email_template.html`` não estiver na pipeline.
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
    """Monta e envia o relatório detalhado do período.

    Reúne assunto, corpo e envio numa chamada só, para que a task correspondente não
    precise conhecer a ordem de composição da mensagem (§4.2 do styleguide).

    :param conexao: Endereço, chave e tempo limite do Data Relay.
    :param destinatarios: Lista de e-mails, que vira ``to_addresses``.
    :param janela: Recorte do relatório.
    :param ocorrencias: Ocorrências do período.
    :param run_url: Endereço da execução na interface do Prefect.
    :raises ValueError: Se a lista de destinatários estiver vazia ou o envio falhar.
    """
    enviar(
        conexao=conexao,
        destinatarios=destinatarios,
        assunto=montar_assunto(janela=janela, ocorrencias=ocorrencias),
        corpo=renderizar(janela=janela, ocorrencias=ocorrencias, run_url=run_url),
    )


def enviar(conexao: ConexaoDataRelay, destinatarios: list[str], assunto: str, corpo: str) -> None:
    """Envia o relatório pelo Data Relay.

    ``is_html_body`` precisa ser ``True``: o padrão do endpoint é ``False``, e sem isso o
    HTML chegaria como texto cru.

    :param conexao: Endereço, chave e tempo limite do Data Relay.
    :param destinatarios: Lista de e-mails, que vira ``to_addresses``.
    :param assunto: Assunto da mensagem.
    :param corpo: Corpo HTML já renderizado.
    :raises ValueError: Se a lista de destinatários estiver vazia ou o envio falhar.
    """
    if not destinatarios:
        raise ValueError("Nenhum destinatário configurado para o relatório.")

    resposta = requests.post(
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
    if resposta.status_code != requests.codes.ok:
        raise ValueError(f"Falha ao enviar o e-mail: {resposta.status_code} - {resposta.text}")

    conteudo = resposta.json()
    if not conteudo.get("success", False):
        raise ValueError(f"Data Relay recusou o envio: {conteudo.get('message')}")
    logger.info("Relatório enviado para %d destinatário(s)", len(destinatarios))
