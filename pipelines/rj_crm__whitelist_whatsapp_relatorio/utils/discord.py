"""Mensagem curta do relatório no Discord.

O Discord recebe só o resumo: confirma que o flow rodou e dá o número do dia. O detalhe
é papel do e-mail (decisão D16).
"""

from dataclasses import dataclass
from datetime import datetime

import requests

from pipelines.rj_crm__whitelist_whatsapp_relatorio.utils.janela import Janela
from pipelines.rj_crm__whitelist_whatsapp_relatorio.utils.log import logger_da_pipeline
from pipelines.rj_crm__whitelist_whatsapp_relatorio.utils.ocorrencias import (
    Ocorrencia,
    descrever_ultima_ocorrencia,
    urls_distintas,
)

logger = logger_da_pipeline(__name__)

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
    """Monta a mensagem do Discord.

    A estrutura é a mesma com e sem ocorrências, mudando apenas as linhas finais: quem
    acompanha o canal reconhece o formato sem reler o cabeçalho.

    Havendo ocorrências, ``destinatarios`` vazio significa **falha no envio do e-mail**:
    ``mailman.enviar`` recusa lista vazia e ``validar_configuracao_task`` já teria
    derrubado o flow se a variável de destinatários não existisse. Essa é a mensagem do
    modo degradado — o relatório sai mesmo com o mailman fora do ar.

    :param janela: Recorte do relatório.
    :param ocorrencias: Ocorrências do período, possivelmente vazia.
    :param destinatarios: Quem recebeu o e-mail detalhado. Vazio quando não houve envio
        por não haver ocorrência, ou quando o envio falhou.
    :param contexto: Ambiente e endereço da execução.
    :param ultima_ocorrencia: Última ocorrência conhecida na fonte, usada só quando o
        período volta vazio. ``None`` quando houve ocorrência ou quando a fonte nunca
        registrou nenhuma.
    :returns: Corpo da mensagem, em Markdown do Discord.
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
        linhas.append(
            "⚠️ **O envio do e-mail falhou** — ninguém recebeu o detalhamento. "
            "As URLs estão na execução acima; reprocesse a janela para reenviar."
        )
    return "\n".join(linhas)


def enviar(webhook_url: str, mensagem: str, timeout: int = 15) -> None:
    """Publica a mensagem no webhook do Discord.

    :param webhook_url: URL do webhook do canal.
    :param mensagem: Corpo já formatado.
    :param timeout: Tempo limite da requisição, em segundos.
    :raises ValueError: Se a mensagem exceder o limite do Discord ou o envio falhar.
    """
    if len(mensagem) > LIMITE_CARACTERES:
        raise ValueError(f"Mensagem excede {LIMITE_CARACTERES} caracteres: {len(mensagem)}.")

    resposta = requests.post(webhook_url, json={"content": mensagem}, params={"wait": "true"}, timeout=timeout)
    if resposta.status_code not in (200, 204):
        raise ValueError(f"Falha ao publicar no Discord: {resposta.status_code} - {resposta.text}")
    logger.info("Mensagem publicada no Discord")
