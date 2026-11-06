"""``HASH_ID`` (R-03): vetores construídos do código VB (``PessoasNacionais.vb``), byte a byte, e mapeamento das pessoas."""

import hashlib

import pytest

from pipelines.rj_smfp__nota_carioca_parser.tests import xmls
from pipelines.rj_smfp__nota_carioca_parser.utils.arvore import ler_xml
from pipelines.rj_smfp__nota_carioca_parser.utils.leitor import No
from pipelines.rj_smfp__nota_carioca_parser.utils.pessoa import Pessoa
from pipelines.rj_smfp__nota_carioca_parser.utils.pessoas import COLUNAS_PESSOA, extrair_pessoas, ler_pessoas

# A ordem dos 23 campos no VB (PessoaNacionalItem.HashId): o texto do vetor é montado campo a campo, sem usar o código.
ORDEM = (
    "cpf_cnpj nif motivo_nif caepf inscricao nome nome_fantasia municipio cep pais postal_ext localidade_ext "
    "cidade_ext logradouro numero complemento bairro telefone email opcao_simples regime_apuracao "
    "regime_tributacao uf"
).split()
CNPJ = "99999999000191"


def vb(**campos: str) -> str:
    base = dict.fromkeys(ORDEM, "NULL") | {"cpf_cnpj": " ", "regime_tributacao": "0"}
    return "|".join((base | campos)[campo] for campo in ORDEM)


VETORES = {
    "cpf_cnpj_vazio": (Pessoa(nome="PESSOA FICTICIA"), vb(nome="PESSOA FICTICIA"), "7230C9B33E3970A5E96D903CC83ACD809068DECC4EFDBD91397E4E54E33D1565"),
    "regime_nulo": (Pessoa(cnpj=CNPJ, nome="EMPRESA FICTICIA"), vb(cpf_cnpj=CNPJ, nome="EMPRESA FICTICIA"), "A2FB1EF875ACC6AB62D1EE686BA776E69C8BAC62F334D75CB162F7E94FCF1B1D"),
    "regime_5": (Pessoa(cnpj=CNPJ, nome="EMPRESA FICTICIA", regime_tributacao=5), vb(cpf_cnpj=CNPJ, nome="EMPRESA FICTICIA", regime_tributacao="5"), "94A5C70FE136901B11706B318F7F3409BE205FA946295F1251D05E1D9AA31917"),
    "email_misto": (Pessoa(cnpj=CNPJ, nome="EMPRESA FICTICIA", email="Fulano.Teste@Example.invalid"), vb(cpf_cnpj=CNPJ, nome="EMPRESA FICTICIA", email="FULANO.TESTE@EXAMPLE.INVALID"), "D3991327A0FF7A29167D9844122FCCFD01043015B32A96CBCE0ED24BF5ED92B4"),
    "telefone_curto": (Pessoa(cnpj=CNPJ, nome="EMPRESA FICTICIA", telefone=None), vb(cpf_cnpj=CNPJ, nome="EMPRESA FICTICIA"), "A2FB1EF875ACC6AB62D1EE686BA776E69C8BAC62F334D75CB162F7E94FCF1B1D"),
    "telefone_valido": (Pessoa(cnpj=CNPJ, nome="EMPRESA FICTICIA", telefone=2133334444), vb(cpf_cnpj=CNPJ, nome="EMPRESA FICTICIA", telefone="2133334444"), "C15EA96933C6538BA876FCF5FFB2D5D48016965ECF41A9F061F53652E8579A1A"),
    "cpf_prevalece_sobre_cnpj": (Pessoa(cpf="99999999999", cnpj=CNPJ, nome="EMPRESA FICTICIA"), vb(cpf_cnpj="99999999999", nome="EMPRESA FICTICIA"), "F72230C70F079D28D36008B4F3CDC28DC32A7B008F01692645537FAF611B131F"),
    "completo": (
        Pessoa(cpf="99999999999", nif="N123", motivo_nif_nao_informado=2, caepf=11122233344, inscricao="IM1", nome="NOME COMPLETO", nome_fantasia="FANTASIA", municipio_ibge=3304557, cep="20000000", codigo_pais="US", codigo_postal_exterior="12345", localidade_exterior="FL", cidade_exterior="MIAMI", logradouro="RUA X", numero="10", complemento="SALA", bairro="CENTRO", telefone=2133334444, email="a@b.com", opcao_simples_nacional=2, regime_apuracao_simples=1, regime_tributacao=3, uf="RJ"),
        vb(cpf_cnpj="99999999999", nif="N123", motivo_nif="2", caepf="11122233344", inscricao="IM1", nome="NOME COMPLETO", nome_fantasia="FANTASIA", municipio="3304557", cep="20000000", pais="US", postal_ext="12345", localidade_ext="FL", cidade_ext="MIAMI", logradouro="RUA X", numero="10", complemento="SALA", bairro="CENTRO", telefone="2133334444", email="A@B.COM", opcao_simples="2", regime_apuracao="1", regime_tributacao="3", uf="RJ"),
        "A049A994FA7A682BE213D3CD02BF726035F86BC2E095D3B4AF1FE05D65CFCDF9",
    ),
}


@pytest.mark.parametrize("nome", VETORES)
def test_hash_texto_do_vetor_bate_com_o_do_vb(nome):
    pessoa, texto_vb, _ = VETORES[nome]
    assert pessoa.texto_do_hash() == texto_vb


@pytest.mark.parametrize("nome", VETORES)
def test_hash_sha256_bate_byte_a_byte_com_o_vetor(nome):
    pessoa, _, esperado = VETORES[nome]
    assert pessoa.hash_hex == esperado
    assert pessoa.hash_id == bytes.fromhex(esperado)
    assert len(pessoa.hash_id) == 32


def test_hash_trocar_a_ordem_de_dois_campos_muda_o_resultado():
    pessoa = Pessoa(cnpj=CNPJ, nome="NOME A", nome_fantasia="NOME B", logradouro="RUA", bairro="CENTRO")
    trocado = pessoa.texto_do_hash().split("|")
    trocado[5], trocado[6] = trocado[6], trocado[5]  # nome <-> nome_fantasia
    assert hashlib.sha256("|".join(trocado).encode()).hexdigest().upper() != pessoa.hash_hex


def test_hash_tem_23_campos_separados_por_pipe():
    assert len(Pessoa().texto_do_hash().split("|")) == len(ORDEM) == 23


def test_hash_email_em_caixa_alta_nao_converte_eszett_como_o_dotnet():
    pessoa = Pessoa(email="straße@example.invalid")
    assert pessoa.texto_do_hash().split("|")[18] == "STRAßE@EXAMPLE.INVALID"


def test_hash_regime_zero_explicito_e_ausente_geram_o_mesmo_hash():
    assert Pessoa(nome="X", regime_tributacao=0).hash_id == Pessoa(nome="X").hash_id


def test_hash_cpf_cnpj_vazio_usa_espaco_e_nao_null_na_linha_tambem():
    assert Pessoa().cpf_cnpj == " "


def no(xml: str) -> No:
    return No(ler_xml(xml))


def test_pessoas_prestador_mapeado_com_endereco_contato_e_regime():
    prestador = ler_pessoas(no(xmls.nfse_xml())).prestador
    assert prestador == Pessoa(
        cnpj=xmls.CNPJ_PRESTADOR,
        inscricao="12345",
        nome="PRESTADOR FICTICIO LTDA",
        municipio_ibge=3304557,
        cep="20000000",
        logradouro="Rua Ficticia",
        numero="100",
        complemento="Sala 1",
        bairro="Centro",
        telefone=2133334444,
        email="ficticio@example.invalid",
        opcao_simples_nacional=1,
        regime_tributacao=0,
    )


def test_pessoas_emitente_tem_uf_e_nome_fantasia_e_nao_tem_regime():
    emitente = ler_pessoas(no(xmls.nfse_xml())).emitente
    assert (emitente.uf, emitente.nome_fantasia, emitente.opcao_simples_nacional) == ("RJ", "EMITENTE FANTASIA", None)
    assert emitente.regime_tributacao is None


def test_pessoas_tomador_cpf_e_sem_regime_tributario():
    tomador = ler_pessoas(no(xmls.nfse_xml(xmls.Nfse(tomador="cpf")))).tomador
    assert (tomador.cpf, tomador.cnpj, tomador.regime_tributacao) == (xmls.CPF_TOMADOR, None, None)


def test_pessoas_sem_tomador_nem_intermediario_ficam_nulos():
    pessoas = ler_pessoas(no(xmls.nfse_xml(xmls.Nfse(tomador=None))))
    assert (pessoas.tomador, pessoas.intermediario) == (None, None)
    assert len(pessoas.todas) == 2


def test_pessoas_endereco_no_exterior_preenche_os_campos_de_fora():
    exterior = (
        "<endExt><cPais>US</cPais><cEndPost>12345</cEndPost><xCidade>Miami</xCidade><xEstProvReg>FL</xEstProvReg></endExt>"
    )
    xml = xmls.nfse_xml().replace("<endNac><cMun>3304557</cMun><CEP>20000000</CEP></endNac>", exterior, 2)
    tomador = ler_pessoas(no(xml)).tomador
    assert (tomador.codigo_pais, tomador.codigo_postal_exterior, tomador.cidade_exterior, tomador.localidade_exterior) == (
        "US",
        "12345",
        "Miami",
        "FL",
    )
    assert (tomador.municipio_ibge, tomador.cep) == (None, None)


def test_pessoas_nome_e_endereco_sao_aparados_e_vazio_vira_nulo():
    xml = xmls.nfse_xml().replace("<xNome>TOMADOR FICTICIO SA</xNome>", "<xNome>  TOMADOR FICTICIO SA  </xNome>")
    xml = xml.replace("<xCpl>Sala 1</xCpl>", "<xCpl>   </xCpl>")
    tomador = ler_pessoas(no(xml)).tomador
    assert (tomador.nome, tomador.complemento) == ("TOMADOR FICTICIO SA", None)


def test_pessoas_linhas_tem_as_colunas_do_oracle_e_o_hash_em_hexadecimal_maiusculo():
    linhas = extrair_pessoas(no(xmls.nfse_xml(xmls.Nfse(intermediario=True))))
    assert len(linhas) == 4
    for linha in linhas:
        assert tuple(linha) == COLUNAS_PESSOA
        assert len(linha["HASH_ID"]) == 64 and linha["HASH_ID"] == linha["HASH_ID"].upper()


def test_pessoas_linha_guarda_o_texto_original_e_o_hash_usa_o_email_em_caixa_alta():
    xml = xmls.nfse_xml().replace("<email>ficticio@example.invalid</email>", "<email>Ficticio@Example.INVALID</email>", 1)
    linha = next(x for x in extrair_pessoas(no(xml)) if x["CPF_CNPJ"] == xmls.CNPJ_EMITENTE and x["UF"] == "RJ")
    assert linha["EMAIL"] == "Ficticio@Example.INVALID"


def test_pessoas_mesma_pessoa_em_dois_papeis_vira_uma_linha_so():
    xml = xmls.nfse_xml(xmls.Nfse(intermediario=True))
    xml = xml.replace(xmls.CNPJ_INTERMEDIARIO, xmls.CNPJ_TOMADOR).replace("INTERMEDIARIO FICTICIO SA", "TOMADOR FICTICIO SA")
    linhas = extrair_pessoas(no(xml))
    assert len(linhas) == 3


@pytest.mark.oracle_leitura
def test_hash_paridade_com_amostras_do_oracle():
    import json
    import os
    from pathlib import Path

    for linha in Path(os.environ["PARIDADE_HASH_AMOSTRAS"]).read_text(encoding="utf-8").splitlines():
        amostra = json.loads(linha)  # uma linha de DFEN.PESSOAS_NACIONAIS com RAWTOHEX(HASH_ID) em "HASH_ID"
        documento = amostra["CPF_CNPJ"].strip()
        pessoa = Pessoa(
            cpf=documento if len(documento) == 11 else None,
            cnpj=documento if len(documento) != 11 and documento else None,
            nif=amostra["NIF"],
            motivo_nif_nao_informado=amostra["MOTIVO_NIF_NAO_INFORMADO"],
            caepf=amostra["CAEPF"],
            inscricao=amostra["INSCRICAO"],
            nome=amostra["NOME"],
            nome_fantasia=amostra["NOME_FANTASIA"],
            municipio_ibge=amostra["MUNICIPIO_IBGE"],
            cep=amostra["CEP"],
            codigo_pais=amostra["CODIGO_PAIS"],
            codigo_postal_exterior=amostra["CODIGO_POSTAL_EXTERIOR"],
            localidade_exterior=amostra["LOCALIDADE_EXTERIOR"],
            cidade_exterior=amostra["CIDADE_EXTERIOR"],
            logradouro=amostra["LOGRADOURO"],
            numero=amostra["NUMERO"],
            complemento=amostra["COMPLEMENTO"],
            bairro=amostra["BAIRRO"],
            telefone=amostra["TELEFONE"],
            email=amostra["EMAIL"],
            opcao_simples_nacional=amostra["OPCAO_SIMPLES_NACIONAL"],
            regime_apuracao_simples=amostra["REGIME_APURACAO_SIMPLES"],
            regime_tributacao=amostra["REGIME_TRIBUTACAO"],
            uf=amostra["UF"],
        )
        assert pessoa.hash_hex == amostra["HASH_ID"].upper()
