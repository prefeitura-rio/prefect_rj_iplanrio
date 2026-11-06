"""Contexto SSL de cliente a partir do PFX do e-CNPJ, sem deixar a chave em disco."""

import base64
import binascii
import os
import ssl
import tempfile
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import pkcs12

from pipelines.rj_smfp__nota_carioca_adn.utils.adn_errors import ConfiguracaoAdnInvalida


def _pem_do_pfx(pfx_base64: str, senha: str) -> bytes:
    """Converte o PFX em PEM (chave sem senha, certificado e cadeia).

    :raises ConfiguracaoAdnInvalida: Se o PFX não for base64, a senha estiver errada ou faltar chave ou certificado.
    """
    try:
        chave, certificado, cadeia = pkcs12.load_key_and_certificates(
            base64.b64decode(pfx_base64, validate=True), senha.encode()
        )
    except (binascii.Error, ValueError) as erro:
        raise ConfiguracaoAdnInvalida("ADN_PFX_BASE64 inválido ou ADN_PFX_SENHA incorreta") from erro
    if chave is None or certificado is None:
        raise ConfiguracaoAdnInvalida("O PFX não tem chave privada e certificado")
    partes = [
        chave.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        ),
        certificado.public_bytes(serialization.Encoding.PEM),
        *(extra.public_bytes(serialization.Encoding.PEM) for extra in cadeia),
    ]
    return b"".join(partes)


def contexto_ssl_cliente(pfx_base64: str, senha: str, ca_bundle: str | None) -> ssl.SSLContext:
    """Monta o ``SSLContext`` de cliente (TLS 1.2 ou superior) com o certificado do PFX.

    O ``ssl`` só carrega certificado e chave de arquivo: o PEM vai para um arquivo temporário de permissão 0600 que é
    apagado no ``finally``, logo depois de o contexto o ler.

    :param pfx_base64: PFX em base64 (``ADN_PFX_BASE64``).
    :param senha: Senha do PFX (``ADN_PFX_SENHA``).
    :param ca_bundle: PEM com as CAs a confiar no servidor; ``None`` usa as do sistema.
    :returns: Contexto pronto para o ``httpx.Client``.
    :raises ConfiguracaoAdnInvalida: Se o PFX ou a senha forem inválidos.
    """
    pem = _pem_do_pfx(pfx_base64, senha)
    contexto = ssl.create_default_context(cafile=ca_bundle)
    contexto.minimum_version = ssl.TLSVersion.TLSv1_2
    descritor, caminho = tempfile.mkstemp(prefix="adn-", suffix=".pem")
    try:
        with os.fdopen(descritor, "wb") as arquivo:
            arquivo.write(pem)
        contexto.load_cert_chain(certfile=caminho)
    finally:
        Path(caminho).unlink(missing_ok=True)
    return contexto
