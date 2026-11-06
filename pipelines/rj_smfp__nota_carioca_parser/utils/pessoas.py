"""Pessoas da NFS-e (emitente, prestador, tomador, intermediário) e as linhas de ``raw_nfse_nacional.pessoas``.

Porta dos construtores de ``PessoaNacionalItem`` (``PessoasNacionais.vb``). O ``PESSOA_NACIONAL`` (GUID gerado pelo
Oracle na inserção) não existe no raw: a chave da pessoa é o ``HASH_ID``, que também preenche ``PESSOA_EMITENTE``,
``PESSOA_PRESTADOR``, ``PESSOA_TOMADOR`` e ``PESSOA_INTERMEDIARIO``.
"""

from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_parser.utils.leitor import (
    No,
    aparado,
    inteiro,
    inteiro_ou_zero,
    texto,
)
from pipelines.rj_smfp__nota_carioca_parser.utils.normalizar import normalizar_cep, normalizar_telefone
from pipelines.rj_smfp__nota_carioca_parser.utils.pessoa import Pessoa

COLUNAS_PESSOA = (
    "CPF_CNPJ",
    "NIF",
    "MOTIVO_NIF_NAO_INFORMADO",
    "CAEPF",
    "INSCRICAO",
    "NOME",
    "NOME_FANTASIA",
    "MUNICIPIO_IBGE",
    "CEP",
    "LOGRADOURO",
    "NUMERO",
    "COMPLEMENTO",
    "BAIRRO",
    "UF",
    "CODIGO_PAIS",
    "CODIGO_POSTAL_EXTERIOR",
    "LOCALIDADE_EXTERIOR",
    "CIDADE_EXTERIOR",
    "TELEFONE",
    "EMAIL",
    "OPCAO_SIMPLES_NACIONAL",
    "REGIME_APURACAO_SIMPLES",
    "REGIME_TRIBUTACAO",
    "HASH_ID",
)


@dataclass(frozen=True, slots=True)
class PessoasDoDocumento:
    """As pessoas da NFS-e; ``None`` onde o XML não traz o grupo."""

    emitente: Pessoa | None
    prestador: Pessoa | None
    tomador: Pessoa | None
    intermediario: Pessoa | None

    @property
    def todas(self) -> list[Pessoa]:
        return [p for p in (self.emitente, self.prestador, self.tomador, self.intermediario) if p is not None]


def _inscricao(no: No) -> str:
    valor = no("IM").texto
    return "" if valor is None or not valor.strip() else valor


def _endereco_comum(end: No) -> dict[str, str | None]:
    return {
        "logradouro": aparado(end("xLgr")),
        "numero": aparado(end("nro")),
        "complemento": aparado(end("xCpl")),
        "bairro": aparado(end("xBairro")),
    }


def _endereco_exterior(end: No) -> dict[str, str | None]:
    exterior = end("endExt")
    if not exterior:
        return {}
    return {
        "codigo_pais": texto(exterior("cPais")),
        "codigo_postal_exterior": texto(exterior("cEndPost")),
        "localidade_exterior": texto(exterior("xEstProvReg")),
        "cidade_exterior": texto(exterior("xCidade")),
    }


def _endereco_dps(end: No) -> dict[str, str | int | None]:
    """``end`` da DPS (``XmlEndereco``): nacional e exterior, sem UF."""
    campos: dict[str, str | int | None] = {}
    if end:
        nacional = end("endNac")
        if nacional:
            campos["municipio_ibge"] = inteiro_ou_zero(nacional("cMun"), "cMun")
            campos["cep"] = normalizar_cep(nacional("CEP").texto)
        campos.update(_endereco_exterior(end))
        campos.update(_endereco_comum(end))
    return campos


def ler_emitente(emit: No) -> Pessoa | None:
    """``PessoaNacionalItem(XmlEmit)``: sem NIF, CAEPF nem regimes; ``UF`` vem do ``enderNac``."""
    if not emit:
        return None
    campos: dict[str, str | int | None] = {}
    ender = emit("enderNac")
    if ender:
        campos |= {
            "municipio_ibge": inteiro_ou_zero(ender("cMun"), "cMun"),
            "cep": normalizar_cep(ender("CEP").texto),
            "uf": texto(ender("UF")),
        } | _endereco_comum(ender)
    return Pessoa(
        cpf=texto(emit("CPF")),
        cnpj=texto(emit("CNPJ")),
        inscricao=_inscricao(emit),
        nome=aparado(emit("xNome")),
        nome_fantasia=aparado(emit("xFant")),
        telefone=normalizar_telefone(emit("fone").texto),
        email=aparado(emit("email")),
        **campos,
    )


def ler_pessoa(no: No, *, com_regime: bool) -> Pessoa | None:
    """``PessoaNacionalItem(XmlInfoPrestador|XmlInfoPessoa)``: ``com_regime`` só no prestador (``regTrib``)."""
    if not no:
        return None
    regime: dict[str, int | None] = {}
    reg = no("regTrib")
    if com_regime and reg:
        regime = {
            "opcao_simples_nacional": inteiro(reg("opSimpNac"), "opSimpNac"),
            "regime_apuracao_simples": inteiro(reg("regApTribSN"), "regApTribSN"),
            "regime_tributacao": inteiro(reg("regEspTrib"), "regEspTrib"),
        }
    return Pessoa(
        cpf=texto(no("CPF")),
        cnpj=texto(no("CNPJ")),
        nif=texto(no("NIF")),
        motivo_nif_nao_informado=inteiro(no("cNaoNIF"), "cNaoNIF"),
        caepf=inteiro(no("CAEPF"), "CAEPF"),
        inscricao=_inscricao(no),
        nome=aparado(no("xNome")),
        telefone=normalizar_telefone(no("fone").texto),
        email=aparado(no("email")),
        **_endereco_dps(no("end")),
        **regime,
    )


def ler_pessoas(raiz: No) -> PessoasDoDocumento:
    """Lê as quatro pessoas de uma ``NFSe`` (``infNFSe/emit`` e ``infNFSe/DPS/infDPS/{prest,toma,interm}``)."""
    inf = raiz("infNFSe")
    dps = inf("DPS", "infDPS")
    return PessoasDoDocumento(
        emitente=ler_emitente(inf("emit")),
        prestador=ler_pessoa(dps("prest"), com_regime=True),
        tomador=ler_pessoa(dps("toma"), com_regime=False),
        intermediario=ler_pessoa(dps("interm"), com_regime=False),
    )


def linha_pessoa(pessoa: Pessoa) -> dict[str, object]:
    """Colunas de ``DFEN.PESSOAS_NACIONAIS`` (sem o GUID) com o valor que o Oracle grava (vazio é ``NULL``)."""
    return {
        "CPF_CNPJ": pessoa.cpf_cnpj,
        "NIF": pessoa.nif or None,
        "MOTIVO_NIF_NAO_INFORMADO": pessoa.motivo_nif_nao_informado,
        "CAEPF": pessoa.caepf,
        "INSCRICAO": pessoa.inscricao or None,
        "NOME": pessoa.nome or None,
        "NOME_FANTASIA": pessoa.nome_fantasia or None,
        "MUNICIPIO_IBGE": pessoa.municipio_ibge,
        "CEP": pessoa.cep or None,
        "LOGRADOURO": pessoa.logradouro or None,
        "NUMERO": pessoa.numero or None,
        "COMPLEMENTO": pessoa.complemento or None,
        "BAIRRO": pessoa.bairro or None,
        "UF": pessoa.uf or None,
        "CODIGO_PAIS": pessoa.codigo_pais or None,
        "CODIGO_POSTAL_EXTERIOR": pessoa.codigo_postal_exterior or None,
        "LOCALIDADE_EXTERIOR": pessoa.localidade_exterior or None,
        "CIDADE_EXTERIOR": pessoa.cidade_exterior or None,
        "TELEFONE": pessoa.telefone,
        "EMAIL": pessoa.email or None,
        "OPCAO_SIMPLES_NACIONAL": pessoa.opcao_simples_nacional,
        "REGIME_APURACAO_SIMPLES": pessoa.regime_apuracao_simples,
        "REGIME_TRIBUTACAO": pessoa.regime_tributacao,
        "HASH_ID": pessoa.hash_hex,
    }


def extrair_pessoas(raiz: No) -> list[dict[str, object]]:
    """Linhas de ``pessoas`` do documento, uma por ``HASH_ID`` (a mesma pessoa em dois papéis aparece uma vez).

    A deduplicação entre documentos é a R-03 do dbt; aqui só se evita repetir a pessoa dentro do mesmo NSU.
    """
    vistas: dict[str, dict[str, object]] = {}
    for pessoa in ler_pessoas(raiz).todas:
        vistas.setdefault(pessoa.hash_hex, linha_pessoa(pessoa))
    return list(vistas.values())
