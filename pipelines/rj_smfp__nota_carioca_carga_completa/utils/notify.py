"""Linhas avulsas da carga completa no Discord (sem mensagem editada), com o webhook das cargas da Nota Carioca.

Os dois filhos já postam o próprio progresso no canal; o orquestrador só avisa início, transição, fim ou parada.
Nada aqui levanta exceção: uma falha de notificação nunca falha a carga completa.
"""

from collections.abc import Callable

import httpx

from pipelines.rj_smfp__nota_carioca_carga_completa.utils.discord import (
    DiscordStatusMessage,
    never_raises,
    resolve_webhook,
    warn,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.discord_format import clip

CONTENT_LIMIT = 2000
NO_MENTIONS = {"parse": []}


def make_notifier(
    enabled: bool,
    resolve: Callable[[], str | None] = resolve_webhook,
    transport: httpx.BaseTransport | None = None,
) -> Callable[[str], None]:
    """Cria a função que posta uma linha avulsa no canal; nunca levanta exceção.

    :param enabled: ``False`` (``discord_notifications=false``) desliga o Discord.
    :param resolve: Lê a URL do webhook (variável do Infisical ou Secret block); ``None`` desliga o Discord.
    :param transport: Transporte do ``httpx`` (os testes passam um ``MockTransport``).
    :returns: Função que recebe o texto da linha.
    """
    try:
        client = DiscordStatusMessage(resolve() if enabled else None, transport=transport)
    except Exception as error:  # ler o webhook nunca pode falhar a carga
        warn(f"Webhook do Discord indisponível: {type(error).__name__}")
        client = DiscordStatusMessage(None)

    @never_raises
    def announce(text: str) -> None:
        client.announce({"content": clip(text, CONTENT_LIMIT), "allowed_mentions": dict(NO_MENTIONS)})

    return announce
