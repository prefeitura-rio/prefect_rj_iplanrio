"""Pessoa nacional e o ``HASH_ID`` (R-03), porta de ``PessoaNacionalItem.HashId`` (``PessoasNacionais.vb``)."""

import hashlib
from dataclasses import dataclass

SEM_DOCUMENTO = " "
SEPARADOR = "|"


def maiusculas(texto: str) -> str:
    """``String.ToUpper()`` do .NET: mapeamento caractere a caractere (``ß`` continua ``ß``, não vira ``SS``)."""
    return "".join(c.upper() if len(c.upper()) == 1 else c for c in texto)


def _ou_null(valor: object) -> str:
    """``If(String.IsNullOrEmpty(x), "NULL", x)`` e ``If(x.HasValue, x.Value.ToString(), "NULL")``."""
    return "NULL" if valor is None or valor == "" else str(valor)


@dataclass(frozen=True, slots=True)
class Pessoa:
    """Os 23 atributos que entram no ``HASH_ID`` (``Cpf`` e ``Cnpj`` são dois campos, mas viram um só no hash)."""

    cpf: str | None = None
    cnpj: str | None = None
    nif: str | None = None
    motivo_nif_nao_informado: int | None = None
    caepf: int | None = None
    inscricao: str | None = None
    nome: str | None = None
    nome_fantasia: str | None = None
    municipio_ibge: int | None = None
    cep: str | None = None
    codigo_pais: str | None = None
    codigo_postal_exterior: str | None = None
    localidade_exterior: str | None = None
    cidade_exterior: str | None = None
    logradouro: str | None = None
    numero: str | None = None
    complemento: str | None = None
    bairro: str | None = None
    telefone: int | None = None
    email: str | None = None
    opcao_simples_nacional: int | None = None
    regime_apuracao_simples: int | None = None
    regime_tributacao: int | None = None
    uf: str | None = None

    @property
    def cpf_cnpj(self) -> str:
        """CPF, senão CNPJ, senão um espaço em branco (``" "``, não ``"NULL"``): é o que o Oracle também grava."""
        return self.cpf or self.cnpj or SEM_DOCUMENTO

    def texto_do_hash(self) -> str:
        """Os 23 campos na ordem do VB, com ``|``: ``" "`` sem CPF/CNPJ, ``"0"`` sem regime, e-mail em maiúsculas."""
        campos = (
            self.cpf_cnpj,
            _ou_null(self.nif),
            _ou_null(self.motivo_nif_nao_informado),
            _ou_null(self.caepf),
            _ou_null(self.inscricao),
            _ou_null(self.nome),
            _ou_null(self.nome_fantasia),
            _ou_null(self.municipio_ibge),
            _ou_null(self.cep),
            _ou_null(self.codigo_pais),
            _ou_null(self.codigo_postal_exterior),
            _ou_null(self.localidade_exterior),
            _ou_null(self.cidade_exterior),
            _ou_null(self.logradouro),
            _ou_null(self.numero),
            _ou_null(self.complemento),
            _ou_null(self.bairro),
            _ou_null(self.telefone),
            maiusculas(self.email) if self.email else "NULL",
            _ou_null(self.opcao_simples_nacional),
            _ou_null(self.regime_apuracao_simples),
            "0" if self.regime_tributacao is None else str(self.regime_tributacao),
            _ou_null(self.uf),
        )
        return SEPARADOR.join(campos)

    @property
    def hash_id(self) -> bytes:
        """SHA-256 do texto do hash em UTF-8 (``RAW(32)`` no Oracle)."""
        return hashlib.sha256(self.texto_do_hash().encode("utf-8")).digest()

    @property
    def hash_hex(self) -> str:
        """``RAWTOHEX(HASH_ID)``: 64 caracteres hexadecimais em maiúsculas."""
        return self.hash_id.hex().upper()
