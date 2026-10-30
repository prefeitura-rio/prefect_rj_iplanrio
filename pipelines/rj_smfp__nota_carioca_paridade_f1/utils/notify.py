"""Linhas avulsas da paridade F1 no Discord, com o webhook das cargas da Nota Carioca.

Mesmo padrão de ``rj_smfp__nota_carioca_carga_completa/utils/notify.py``: em staging a linha vai para o webhook de
staging e leva o marcador ``🧪 STAGING · ``. Nada aqui levanta exceção: uma falha de notificação nunca falha o flow.
"""

from collections.abc import Callable
from functools import partial

import httpx

from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.discord import (
    DiscordStatusMessage,
    Environment,
    never_raises,
    resolve_webhook,
    warn,
)
from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.render import clip

STAGING_MARK = "🧪 STAGING · "
NO_MENTIONS = {"parse": []}


def make_notifier(
    enabled: bool,
    environment: Environment = Environment.PROD,
    resolve: Callable[[], str | None] | None = None,
    transport: httpx.BaseTransport | None = None,
) -> Callable[[str], None]:
    """Cria a função que posta uma linha avulsa no canal; nunca levanta exceção.

    :param enabled: ``False`` (``discord_notifications=false``) desliga o Discord.
    :param environment: Ambiente do deployment: staging usa o webhook de staging e marca as linhas com 🧪 STAGING.
    :param resolve: Lê a URL do webhook; o padrão lê o do ambiente (variável do Infisical ou Secret block).
    :param transport: Transporte do ``httpx`` (os testes passam um ``MockTransport``).
    :returns: Função que recebe o texto da linha.
    """
    staging = environment is Environment.STAGING
    read_webhook = resolve if resolve is not None else partial(resolve_webhook, environment=environment)
    try:
        client = DiscordStatusMessage(read_webhook() if enabled else None, transport=transport)
    except Exception as error:  # ler o webhook nunca pode falhar o flow
        warn(f"Webhook do Discord indisponível: {type(error).__name__}")
        client = DiscordStatusMessage(None)

    @never_raises
    def announce(text: str) -> None:
        marked = f"{STAGING_MARK}{text}" if staging else text
        client.announce({"content": clip(marked), "allowed_mentions": dict(NO_MENTIONS)})

    return announce
