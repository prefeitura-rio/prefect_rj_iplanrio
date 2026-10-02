# -*- coding: utf-8 -*-
"""
Testes para o host key pinning do send_to_sftp (CHATA-140).

- test_host_key_algs_*: unitários, sempre rodam, sem rede.
- test_normalize_host_key_value_*: unitários, sempre rodam, sem rede.
- test_send_to_sftp_raises_*: unitários, sempre rodam, sem rede -- a
  validação de csv_path acontece antes de qualquer conexão, então nem
  precisa de credenciais reais pra testar.
- test_sftp_connect_*/test_send_to_sftp_uploads_successfully: de integração
  contra o SFTP real de homologação da Salesforce. Pulados automaticamente
  se as credenciais não estiverem no ambiente (mesmas variáveis que o
  Infisical injeta em produção: sf_sftp_host, sf_sftp_user, sf_sftp_password,
  sf_sftp_host_key, opcionalmente sf_sftp_port).
"""
import asyncio
import os
import tempfile
from pathlib import Path

import asyncssh
import pytest

from pipelines.rj_crm__disparo_template.utils.dispatch import (
    host_key_algs_for_pinned_key,
    normalize_host_key_value,
    send_to_sftp,
)

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


# --- normalize_host_key_value (formato do secret sf_sftp_host_key) ------

_PUB_LINE = RSA_TEST_KEY.export_public_key().decode().strip()


def test_normalize_host_key_value_accepts_bare_key_unchanged():
    # Formato já aceito hoje -- não pode mudar de comportamento.
    assert normalize_host_key_value(_PUB_LINE) == _PUB_LINE


def test_normalize_host_key_value_strips_trailing_comment():
    # Formato de arquivo .pub real: "ssh-rsa AAAA... user@host"
    with_comment = f"{_PUB_LINE} usuario@algumamaquina"
    assert normalize_host_key_value(with_comment) == _PUB_LINE


def test_normalize_host_key_value_converts_known_hosts_line():
    # A causa raiz do CHATA-140: colar a saída do ssh-keyscan (que sempre
    # inclui o hostname na frente) direto no secret quebrava a importação.
    known_hosts_line = f"meuhost.com.br {_PUB_LINE}"
    normalized = normalize_host_key_value(known_hosts_line)

    assert normalized == _PUB_LINE
    # e o resultado tem que ser importável de verdade, não só "parecer certo"
    imported = asyncssh.import_public_key(normalized)
    assert imported.export_public_key() == RSA_TEST_KEY.export_public_key()


@pytest.mark.parametrize(
    "prefix",
    [
        "meuhost.com.br",
        "meuhost.com.br,192.0.2.10",  # known_hosts com múltiplos hosts/IP
        "[meuhost.com.br]:2222",  # known_hosts com porta não-padrão
    ],
)
def test_normalize_host_key_value_strips_various_hostname_formats(prefix):
    line = f"{prefix} {_PUB_LINE}"
    assert normalize_host_key_value(line) == _PUB_LINE


# --- send_to_sftp: validação de csv_path (CHATA-140) --------------------
# A checagem roda como a primeira linha da função, antes de qualquer coisa
# relacionada a rede/credenciais -- por isso esses dois testes rodam sempre,
# sem precisar de sftp_host_key nem de conexão nenhuma.


def test_send_to_sftp_raises_for_missing_csv():
    with pytest.raises(FileNotFoundError):
        send_to_sftp.fn(csv_path="/caminho/que/definitivamente/nao/existe_chata140.csv")


def test_send_to_sftp_raises_for_directory_as_csv_path(tmp_path):
    # os.path.isfile() rejeita diretórios -- "exigir que o caminho seja um
    # arquivo regular" era um critério de aceite explícito do CHATA-140.
    with pytest.raises(FileNotFoundError):
        send_to_sftp.fn(csv_path=str(tmp_path))


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
def test_sftp_connect_succeeds_with_known_hosts_style_secret_value():
    """Reproduz o cenário do CHATA-140: se sf_sftp_host_key tivesse sido
    cadastrado como uma linha completa de ssh-keyscan/known_hosts (hostname +
    chave), a conexão real ainda deve funcionar depois de
    normalize_host_key_value()."""
    cfg = _sftp_config()
    known_hosts_style_value = f"{cfg['host']} {cfg['host_key']}"
    pinned_key = asyncssh.import_public_key(normalize_host_key_value(known_hosts_style_value))

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
def test_send_to_sftp_uploads_successfully():
    """Chama send_to_sftp() de verdade (não só o handshake asyncssh cru) com
    um CSV real contra o SFTP de homologação -- deve completar sem lançar
    nada quando o arquivo existe e o upload dá certo (CHATA-140: garante que
    o caminho de sucesso continua funcionando depois de mover a validação
    de csv_path pra antes da conexão)."""
    cfg = _sftp_config()
    with tempfile.TemporaryDirectory() as tmp_dir:
        csv_path = Path(tmp_dir) / "teste_chata140.csv"
        csv_path.write_text("telefone;SubscriberKey\n5521999999999;00000000000\n")

        send_to_sftp.fn(
            csv_path=str(csv_path),
            sftp_host=cfg["host"],
            sftp_port=cfg["port"],
            sftp_user=cfg["user"],
            sftp_password=cfg["password"],
            sftp_host_key=cfg["host_key"],
            sftp_remote_path="/Import/",
        )


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
