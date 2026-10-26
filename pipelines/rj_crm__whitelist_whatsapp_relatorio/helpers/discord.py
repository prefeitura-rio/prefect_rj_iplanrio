"""Mensagem curta do relatório no Discord.

O Discord recebe só o resumo: confirma que o flow rodou e dá o número do dia. O detalhe
é papel do e-mail (decisão D16).
"""

from dataclasses import dataclass
from datetime import datetime

from httpx2 import post
from toolkit.telemetry import logger

from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.janela import Janela
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.ocorrencias import (
    Ocorrencia,
    descrever_ultima_ocorrencia,
    urls_distintas,
)

LIMITE_CARACTERES = 2000
"""Teto de um `content` no webhook do Discord."""

TITULO = "📋 Relatório diário — URLs redigidas"
NOME_FLOW = "rj_crm__whitelist_whatsapp_relatorio"


@dataclass(frozen=True)
class Contexto:
    """Dados da execução que aparecem no cabeçalho da mensagem."""

    ambiente: str
    run_url: str


def montar_mensagem(
    janela: Janela,
    ocorrencias: list[Ocorrencia],
    destinatarios: list[str],
    contexto: Contexto,
    ultima_ocorrencia: datetime | None,
) -> str:
    """Build the Discord message.

    The layout is the same with and without occurrences, and only the last lines change,
    so a reader of the channel knows the format without rereading the header.

    With occurrences, an empty `destinatarios` means that **the e-mail failed**:
    `mailman.enviar` refuses an empty list, and `validar_configuracao_task` would have
    stopped the flow if the variable of the recipients did not exist. This is the
    message of the degraded mode: the report goes out even if the mailman is down.

    `ultima_ocorrencia` is the last occurrence that the source knows. It is used only
    when the period comes back empty, and it is `None` when there was an occurrence or
    when the source never had one. Return the body in Discord Markdown.
    """
    linhas = [
        f"## {TITULO}",
        f"> Janela: {janela.inicio_exibicao} → {janela.fim_exibicao}",
        f"> Ambiente: {contexto.ambiente}",
    ]
    if contexto.run_url:
        linhas.append(f"> Execução: [{NOME_FLOW}]({contexto.run_url})")
    linhas.append("")

    if not ocorrencias:
        linhas.append("Nenhuma ocorrência no período.")
        linhas.append(descrever_ultima_ocorrencia(ultima_ocorrencia, janela.fim))
        return "\n".join(linhas)

    total_urls = len(urls_distintas(ocorrencias))
    linhas.append(f"**{len(ocorrencias)}** ocorrências no período, com {total_urls} endereços distintos.")
    if destinatarios:
        linhas.append(f"Detalhamento enviado para: {', '.join(destinatarios)}")
    else:
        linhas.append("⚠️ **O envio do e-mail falhou** — ninguém recebeu o detalhamento.")
        linhas.append("As URLs estão na execução acima; reprocesse a janela para reenviar.")
    return "\n".join(linhas)


def enviar(webhook_url: str, mensagem: str, timeout: int = 15) -> None:
    """Post the message to the Discord webhook.

    Raise `ValueError` if the message is over the limit of Discord or the post fails.
    `timeout` is in seconds.
    """
    if len(mensagem) > LIMITE_CARACTERES:
        raise ValueError(f"Mensagem excede {LIMITE_CARACTERES} caracteres: {len(mensagem)}.")

    resposta = post(webhook_url, json={"content": mensagem}, params={"wait": "true"}, timeout=timeout)
    if resposta.status_code not in (200, 204):
        raise ValueError(f"Falha ao publicar no Discord: {resposta.status_code} - {resposta.text}")
    logger.info("Mensagem publicada no Discord")
