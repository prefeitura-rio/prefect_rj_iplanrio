"""Linhas avulsas da carga completa no Discord (sem mensagem editada), com o webhook das cargas da Nota Carioca.

Os filhos já postam o próprio progresso no canal; o orquestrador só avisa início, transição, fim ou parada. As paradas
saem por um notificador de alerta, que menciona os cargos configurados (``<@&id>``) e só permite essas menções.
Em staging a linha vai para o webhook de staging e leva o marcador ``🧪 STAGING · ``.
Nada aqui levanta exceção: uma falha de notificação nunca falha a carga completa.
"""

from collections.abc import Callable, Sequence
from functools import partial

import httpx

from pipelines.rj_smfp__nota_carioca_carga_completa.utils.discord import (
    DiscordStatusMessage,
    Environment,
    never_raises,
    resolve_webhook,
    warn,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.discord_format import clip, mark_staging

CONTENT_LIMIT = 2000
NO_MENTIONS = {"parse": []}


def make_notifier(
    enabled: bool,
    environment: Environment = Environment.PROD,
    resolve: Callable[[], str | None] | None = None,
    transport: httpx.BaseTransport | None = None,
    mention_role_ids: Sequence[str] = (),
) -> Callable[[str], None]:
    """Cria a função que posta uma linha avulsa no canal; nunca levanta exceção.

    :param enabled: ``False`` (``discord_notifications=false``) desliga o Discord.
    :param environment: Ambiente do deployment: staging usa o webhook de staging e prefixa as linhas com
        ``🧪 STAGING · ``; o padrão é prod.
    :param resolve: Lê a URL do webhook; o padrão lê o do ambiente (variável do Infisical ou Secret block). ``None``
        como resultado desliga o Discord.
    :param transport: Transporte do ``httpx`` (os testes passam um ``MockTransport``).
    :param mention_role_ids: Ids dos cargos do Discord mencionados no começo de cada linha (alerta); vazio não menciona.
    :returns: Função que recebe o texto da linha.
    :raises ValueError: Se algum id de cargo não for numérico.
    """
    roles = [str(role).strip() for role in mention_role_ids]
    invalid = [role for role in roles if not role.isdigit()]
    if invalid:
        raise ValueError(f"Id de cargo do Discord inválido (use o número do cargo, não o nome): {invalid}")
    prefix = "".join(f"<@&{role}> " for role in roles)
    mentions: dict[str, list[str]] = {**NO_MENTIONS, "roles": roles} if roles else dict(NO_MENTIONS)
    staging = environment is Environment.STAGING
    read_webhook = resolve if resolve is not None else partial(resolve_webhook, environment=environment)
    try:
        client = DiscordStatusMessage(read_webhook() if enabled else None, transport=transport)
    except Exception as error:  # ler o webhook nunca pode falhar a carga
        warn(f"Webhook do Discord indisponível: {type(error).__name__}")
        client = DiscordStatusMessage(None)

    @never_raises
    def announce(text: str) -> None:
        content = clip(prefix + mark_staging(text, staging), CONTENT_LIMIT)
        client.announce({"content": content, "allowed_mentions": dict(mentions)})

    return announce
