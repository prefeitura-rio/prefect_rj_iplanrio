# -*- coding: utf-8 -*-
"""
Testes para o host key pinning do send_to_sftp (CHATA-140).

- test_host_key_algs_*: unitários, sempre rodam, sem rede.
- test_sftp_connect_*: de integração contra o SFTP real de homologação da
  Salesforce. Pulados automaticamente se as credenciais não estiverem no
  ambiente (mesmas variáveis que o Infisical injeta em produção: sf_sftp_host,
  sf_sftp_user, sf_sftp_password, sf_sftp_host_key, opcionalmente sf_sftp_port).
"""
import asyncio
import os

import asyncssh
import pytest

from pipelines.rj_crm__disparo_template.utils.dispatch import host_key_algs_for_pinned_key

# --- unitários (sem rede) -----------------------------------------------

RSA_TEST_KEY = asyncssh.generate_private_key("ssh-rsa")
ED25519_TEST_KEY = asyncssh.generate_private_key("ssh-ed25519")
ECDSA_TEST_KEY = asyncssh.generate_private_key("ecdsa-sha2-nistp256")


def test_host_key_algs_rsa_prefers_sha2_over_sha1():
    algs = host_key_algs_for_pinned_key(RSA_TEST_KEY)

    assert algs == ["rsa-sha2-512", "rsa-sha2-256", "ssh-rsa"]
    # ssh-rsa (SHA-1) só pode aparecer como último recurso, nunca antes das
    # variantes SHA-2 -- é exatamente o bug reportado no CHATA-140.
    assert algs.index("ssh-rsa") > algs.index("rsa-sha2-256") > -1
    assert algs.index("ssh-rsa") > algs.index("rsa-sha2-512") > -1


@pytest.mark.parametrize("key", [ED25519_TEST_KEY, ECDSA_TEST_KEY])
def test_host_key_algs_non_rsa_key_uses_its_own_algorithm(key):
    # Chaves ed25519/ecdsa não têm variante SHA-1 vs SHA-2 pra escolher --
    # devem seguir exatamente como get_algorithm() já reporta.
    assert host_key_algs_for_pinned_key(key) == [key.get_algorithm()]


# --- integração contra o SFTP real de homologação -----------------------

_REQUIRED_ENV = ["sf_sftp_host", "sf_sftp_user", "sf_sftp_password", "sf_sftp_host_key"]
_missing_env = [v for v in _REQUIRED_ENV if not os.getenv(v)]

pytestmark_integration = pytest.mark.skipif(
    bool(_missing_env),
    reason=f"Variáveis de ambiente ausentes para teste de integração: {_missing_env}",
)


def _sftp_config():
    return {
        "host": os.environ["sf_sftp_host"],
        "port": int(os.getenv("sf_sftp_port", "22")),
        "user": os.environ["sf_sftp_user"],
        "password": os.environ["sf_sftp_password"],
        "host_key": os.environ["sf_sftp_host_key"],
    }


@pytestmark_integration
def test_sftp_connect_succeeds_with_pinned_key():
    """Conecta no SFTP de homologação com a chave real fixada -- deve funcionar,
    e a negociação deve preferir rsa-sha2-* quando o servidor suportar."""
    cfg = _sftp_config()
    pinned_key = asyncssh.import_public_key(cfg["host_key"])

    async def _connect():
        async with asyncssh.connect(
            cfg["host"],
            port=cfg["port"],
            username=cfg["user"],
            password=cfg["password"],
            server_host_key_algs=host_key_algs_for_pinned_key(pinned_key),
            known_hosts=([pinned_key], [], []),
            connect_timeout=15,
        ) as conn:
            return conn.get_server_host_key()

    server_key = asyncio.run(_connect())
    assert server_key.export_public_key() == pinned_key.export_public_key()


@pytestmark_integration
def test_sftp_connect_rejects_different_key():
    """Uma chave diferente da real deve ser recusada -- é a proteção central
    contra MITM que esse pinning existe pra garantir."""
    cfg = _sftp_config()
    wrong_key = asyncssh.generate_private_key("ssh-rsa")  # nunca vai bater com o servidor real

    async def _connect():
        async with asyncssh.connect(
            cfg["host"],
            port=cfg["port"],
            username=cfg["user"],
            password=cfg["password"],
            server_host_key_algs=host_key_algs_for_pinned_key(wrong_key),
            known_hosts=([wrong_key], [], []),
            connect_timeout=15,
        ):
            pass

    with pytest.raises(asyncssh.HostKeyNotVerifiable):
        asyncio.run(_connect())
