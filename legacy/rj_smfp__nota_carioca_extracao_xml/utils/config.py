"""Configuração lida do ambiente: token do webservice, conexão do Oracle e endereço do webservice.

O ``InfisicalSecret`` do Prefect copia cada chave do projeto (recursivo) para o pod com o nome literal da chave. As
chaves do Oracle já são gravadas com o prefixo da pasta no nome, na convenção da lib ``iplanrio`` (``/db-oracle-x`` +
``DB_HOST`` → ``DB_ORACLE_X__DB_HOST``); o token do webservice foi gravado como ``NOTA_CARIOCA_ROTINAS_TOKEN``, sem
prefixo. Prod e staging usam as mesmas chaves, cada um com o valor do seu ambiente (staging = homologação).
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import assert_never

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import PROXY_URL, TOKEN_KEY, URL_PROD, URL_STAGING
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.discord import Environment


@dataclass(frozen=True)
class OracleConfig:
    """Conexão com o Oracle da aplicação, só para ler ``NFSE_OWNER.TAREFAS``.

    :param user: Usuário do banco.
    :param password: Senha do usuário (fora do ``repr``).
    :param host: Host ou IP do banco.
    :param port: Porta do listener.
    :param service_name: Service name do banco.
    """

    user: str
    password: str = field(repr=False)
    host: str
    port: str
    service_name: str

    @property
    def dsn(self) -> str:
        """DSN no formato easy connect (``host:porta/service``)."""
        return f"{self.host}:{self.port}/{self.service_name}"


def secret_env_key(infisical_secret_path: str, key: str) -> str:
    """Nome da variável de ambiente de uma chave do Infisical (``/db-oracle-x`` + ``K`` → ``DB_ORACLE_X__K``).

    :param infisical_secret_path: Pasta do segredo no Infisical.
    :param key: Nome da chave.
    :returns: Nome da variável de ambiente.
    """
    prefix = infisical_secret_path.upper().replace("-", "_").replace("/", "")
    return f"{prefix}__{key}"


def _required(infisical_secret_path: str, key: str, environ: Mapping[str, str] | None) -> str:
    name = secret_env_key(infisical_secret_path, key)
    value = (environ if environ is not None else os.environ).get(name, "").strip()
    if not value:
        raise ValueError(
            f"Variável {name} ausente ou vazia (chave {key} da pasta {infisical_secret_path} do Infisical)."
        )
    return value


def read_token(environ: Mapping[str, str] | None = None) -> str:
    """Lê o token do webservice da variável ``NOTA_CARIOCA_ROTINAS_TOKEN`` (nome literal da chave no Infisical).

    :param environ: Variáveis de ambiente; o padrão é ``os.environ``.
    :returns: O token, sem espaços nas pontas.
    :raises ValueError: Se a variável estiver ausente ou vazia.
    """
    value = (environ if environ is not None else os.environ).get(TOKEN_KEY, "").strip()
    if not value:
        raise ValueError(f"Variável {TOKEN_KEY} ausente ou vazia (chave {TOKEN_KEY} do Infisical do Prefect).")
    return value


def read_oracle_config(infisical_secret_path: str, environ: Mapping[str, str] | None = None) -> OracleConfig:
    """Lê a conexão do Oracle da mesma pasta do Infisical que o ``oracle_to_bq`` usa.

    :param infisical_secret_path: Pasta do segredo no Infisical.
    :param environ: Variáveis de ambiente; o padrão é ``os.environ``.
    :returns: A configuração da conexão.
    :raises ValueError: Se alguma variável estiver ausente ou vazia.
    """
    return OracleConfig(
        user=_required(infisical_secret_path, "DB_USERNAME", environ),
        password=_required(infisical_secret_path, "DB_PASSWORD", environ),
        host=_required(infisical_secret_path, "DB_HOST", environ),
        port=_required(infisical_secret_path, "DB_PORT", environ),
        service_name=_required(infisical_secret_path, "DB_SERVICE_NAME", environ),
    )


def resolve_proxy(proxy_url: str | None) -> str | None:
    """Proxy HTTP da chamada ao webservice: ``None`` usa o squid do cluster; vazio chama direto.

    :param proxy_url: Valor do parâmetro do flow.
    :returns: A URL do proxy, ou ``None`` para chamar sem proxy.
    :raises ValueError: Se o proxy informado não for ``http://`` ou ``https://``.
    """
    if proxy_url is None:
        return PROXY_URL
    value = proxy_url.strip()
    if not value:
        return None
    if not value.startswith(("http://", "https://")):
        raise ValueError(f"O proxy precisa ser http:// ou https://, veio {proxy_url!r}.")
    return value


def resolve_url(url: str | None, environment: Environment) -> str:
    """Endereço do webservice: o informado (só ``https``) ou o do ambiente (staging = homologação).

    :param url: Endereço informado no parâmetro do flow, ou ``None``.
    :param environment: Ambiente do deployment.
    :returns: O endereço do ``RotinasAutomaticas.asmx``.
    :raises ValueError: Se o endereço informado não for ``https``.
    """
    if url:
        if not url.startswith("https://"):
            raise ValueError(f"O endereço do webservice precisa ser https, veio {url!r}.")
        return url
    match environment:
        case Environment.PROD:
            return URL_PROD
        case Environment.STAGING:
            return URL_STAGING
        case unreachable:
            assert_never(unreachable)
