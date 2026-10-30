"""Infraestrutura de chaves de teste: uma CA autoassinada, um servidor e um cliente (PFX) gerados na hora."""

import base64
import ipaddress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

SENHA_PFX = "senha-de-teste-do-pfx"
CN_CLIENTE = "PREFEITURA SINTETICA:99999999000199"


@dataclass(frozen=True)
class Pki:
    """Arquivos e valores da PKI de teste.

    :param ca_pem: Caminho do certificado da CA (usado pelo cliente para confiar no servidor e pelo servidor para
        exigir certificado de cliente).
    :param servidor_cert: Caminho do certificado do servidor.
    :param servidor_chave: Caminho da chave do servidor.
    :param pfx_base64: PFX do cliente em base64, como na variável ``ADN_PFX_BASE64``.
    :param senha: Senha do PFX.
    :param cliente_pem: Certificado e chave do cliente num PEM só, para quem precisa de um cliente HTTP "cru".
    """

    ca_pem: Path
    servidor_cert: Path
    servidor_chave: Path
    pfx_base64: str
    senha: str
    cliente_pem: Path


def _nome(cn: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def _emitir(
    chave_publica: object, assunto: str, emissor: x509.Name, chave_emissor: object, extras: list[x509.Extension]
) -> x509.Certificate:
    agora = datetime.now(UTC)
    construtor = (
        x509.CertificateBuilder()
        .subject_name(_nome(assunto))
        .issuer_name(emissor)
        .public_key(chave_publica)  # type: ignore[arg-type]
        .serial_number(x509.random_serial_number())
        .not_valid_before(agora - timedelta(minutes=5))
        .not_valid_after(agora + timedelta(days=2))
    )
    # O Python 3.13 valida em modo estrito e exige os identificadores de chave (SKI e AKI).
    construtor = construtor.add_extension(x509.SubjectKeyIdentifier.from_public_key(chave_publica), False)  # type: ignore[arg-type]
    construtor = construtor.add_extension(
        x509.AuthorityKeyIdentifier.from_issuer_public_key(chave_emissor.public_key()),  # type: ignore[attr-defined]
        False,
    )
    for extensao in extras:
        construtor = construtor.add_extension(extensao.value, extensao.critical)
    return construtor.sign(chave_emissor, hashes.SHA256())  # type: ignore[arg-type]


def _pem(certificado: x509.Certificate) -> bytes:
    return certificado.public_bytes(serialization.Encoding.PEM)


def gerar_pki(diretorio: Path) -> Pki:
    """Gera CA, servidor (``localhost`` e ``127.0.0.1``) e cliente (RSA, em PFX com senha) em ``diretorio``."""
    chave_ca = ec.generate_private_key(ec.SECP256R1())
    nome_ca = _nome("CA SINTETICA DE TESTE")
    ca = _emitir(
        chave_ca.public_key(),
        "CA SINTETICA DE TESTE",
        nome_ca,
        chave_ca,
        [
            x509.Extension(x509.ExtensionOID.BASIC_CONSTRAINTS, True, x509.BasicConstraints(ca=True, path_length=None)),
            x509.Extension(
                x509.ExtensionOID.KEY_USAGE,
                True,
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=True,
                    crl_sign=True,
                    encipher_only=False,
                    decipher_only=False,
                ),
            ),
        ],
    )
    chave_servidor = ec.generate_private_key(ec.SECP256R1())
    alt = x509.SubjectAlternativeName([x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))])
    servidor = _emitir(
        chave_servidor.public_key(),
        "localhost",
        nome_ca,
        chave_ca,
        [
            x509.Extension(x509.ExtensionOID.SUBJECT_ALTERNATIVE_NAME, False, alt),
            x509.Extension(
                x509.ExtensionOID.EXTENDED_KEY_USAGE, False, x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH])
            ),
        ],
    )
    chave_cliente = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    cliente = _emitir(
        chave_cliente.public_key(),
        CN_CLIENTE,
        nome_ca,
        chave_ca,
        [
            x509.Extension(
                x509.ExtensionOID.EXTENDED_KEY_USAGE, False, x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH])
            )
        ],
    )
    pfx = pkcs12.serialize_key_and_certificates(
        b"cliente",
        chave_cliente,
        cliente,
        [ca],
        serialization.BestAvailableEncryption(SENHA_PFX.encode()),
    )
    ca_pem = diretorio / "ca.pem"
    ca_pem.write_bytes(_pem(ca))
    servidor_cert = diretorio / "servidor.pem"
    servidor_cert.write_bytes(_pem(servidor))
    servidor_chave = diretorio / "servidor.key"
    servidor_chave.write_bytes(
        chave_servidor.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
    )
    cliente_pem = diretorio / "cliente.pem"
    cliente_pem.write_bytes(
        chave_cliente.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
        + _pem(cliente)
    )
    return Pki(ca_pem, servidor_cert, servidor_chave, base64.b64encode(pfx).decode(), SENHA_PFX, cliente_pem)
