"""Documentos XML sintéticos para os testes, válidos pelos XSDs oficiais e sem nenhum dado real.

CPF e CNPJ são gerados com dígito verificador válido a partir de bases marcadamente fictícias (``99999999`` e
``999999999``); nomes, e-mails e endereços são inventados. Os builders devolvem ``str``; ``mutar`` altera um nó por XPath
para criar o defeito de um teste sem reescrever o documento.
"""

from dataclasses import dataclass, field

from lxml import etree

NS = "http://www.sped.fazenda.gov.br/nfse"
NSMAP = {"n": NS}


def digito_cnpj(base12: str) -> str:
    """Calcula os dois dígitos verificadores de um CNPJ numérico a partir dos 12 primeiros."""
    pesos = ([5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2], [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    digitos = base12
    for peso in pesos:
        soma = sum(int(d) * p for d, p in zip(digitos, peso, strict=False))
        resto = soma % 11
        digitos += "0" if resto < 2 else str(11 - resto)
    return digitos[12:]


def digito_cpf(base9: str) -> str:
    """Calcula os dois dígitos verificadores de um CPF a partir dos 9 primeiros."""
    digitos = base9
    for tamanho in (9, 10):
        soma = sum(int(d) * (tamanho + 1 - i) for i, d in enumerate(digitos[:tamanho]))
        resto = (soma * 10) % 11
        digitos += "0" if resto >= 10 else str(resto)
    return digitos[9:]


CNPJ_PRESTADOR = "999999990001" + digito_cnpj("999999990001")
CNPJ_TOMADOR = "999999990002" + digito_cnpj("999999990002")
CNPJ_INTERMEDIARIO = "999999990003" + digito_cnpj("999999990003")
CNPJ_EMITENTE = CNPJ_PRESTADOR
CPF_TOMADOR = "999999999" + digito_cpf("999999999")
CPF_AUTOR = "999999998" + digito_cpf("999999998")

CHAVE_NFSE = "3304557" + "2" + "2" + CNPJ_PRESTADOR + "0000000000123" + "2602" + "123456789" + "0"
CHAVE_SUBSTITUTA = "3304557" + "2" + "2" + CNPJ_PRESTADOR + "0000000000124" + "2602" + "123456780" + "9"
ID_DPS = "DPS" + "3304557" + "2" + CNPJ_PRESTADOR + "00001" + "000000000000123"

ASSINATURA = (
    '<Signature xmlns="http://www.w3.org/2000/09/xmldsig#"><SignedInfo>'
    '<CanonicalizationMethod Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"/>'
    '<SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/>'
    '<Reference URI="#{ref}"><Transforms>'
    '<Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>'
    '<Transform Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"/></Transforms>'
    '<DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"/><DigestValue>AAAAAAAAAAAAAAAAAAAAAAAAAAA=</DigestValue>'
    "</Reference></SignedInfo><SignatureValue>AAAA</SignatureValue>"
    "<KeyInfo><X509Data><X509Certificate>AAAA</X509Certificate></X509Data></KeyInfo></Signature>"
)


@dataclass(frozen=True)
class Nfse:
    """Parâmetros do documento NFS-e sintético; o padrão é uma nota de ISS próprio, Rio, com tomador CNPJ."""

    versao: str = "1.01"
    estrutura: str = ""  # estrutura dos elementos quando difere da versão declarada (1.00 com a estrutura da 1.01)
    tp_ret: int = 1
    tomador: str | None = "cnpj"
    intermediario: bool = False
    ibscbs: bool = False
    ded_red: str | None = None
    extra_inf_nfse: str = ""
    extras: dict[str, str] = field(default_factory=dict)


def _endereco() -> str:
    return (
        "<end><endNac><cMun>3304557</cMun><CEP>20000000</CEP></endNac><xLgr>Rua Ficticia</xLgr><nro>100</nro>"
        "<xCpl>Sala 1</xCpl><xBairro>Centro</xBairro></end>"
    )


def _pessoa(tag: str, doc_tag: str, doc: str, nome: str, *, regtrib: bool = False) -> str:
    reg = "<regTrib><opSimpNac>1</opSimpNac><regEspTrib>0</regEspTrib></regTrib>" if regtrib else ""
    return (
        f"<{tag}><{doc_tag}>{doc}</{doc_tag}><IM>12345</IM><xNome>{nome}</xNome>{_endereco()}"
        f"<fone>2133334444</fone><email>ficticio@example.invalid</email>{reg}</{tag}>"
    )


def _emitente() -> str:
    return (
        f"<emit><CNPJ>{CNPJ_EMITENTE}</CNPJ><IM>12345</IM><xNome>EMITENTE FICTICIO LTDA</xNome>"
        "<xFant>EMITENTE FANTASIA</xFant><enderNac><xLgr>Rua Ficticia</xLgr><nro>100</nro><xCpl>Sala 1</xCpl>"
        "<xBairro>Centro</xBairro><cMun>3304557</cMun><UF>RJ</UF><CEP>20000000</CEP></enderNac>"
        "<fone>2133334444</fone><email>ficticio@example.invalid</email></emit>"
    )


def _dps(p: Nfse) -> str:
    toma = ""
    if p.tomador == "cnpj":
        toma = _pessoa("toma", "CNPJ", CNPJ_TOMADOR, "TOMADOR FICTICIO SA", regtrib=False)
    elif p.tomador == "cpf":
        toma = _pessoa("toma", "CPF", CPF_TOMADOR, "TOMADOR PESSOA FISICA FICTICIA", regtrib=False)
    interm = _pessoa("interm", "CNPJ", CNPJ_INTERMEDIARIO, "INTERMEDIARIO FICTICIO SA", regtrib=False)
    ded = p.ded_red or ""
    # A ordem de pAliq e tpRetISSQN muda entre as versões do XSD (v1.00: pAliq antes; v1.01: depois).
    ret, aliq = f"<tpRetISSQN>{p.tp_ret}</tpRetISSQN>", "<pAliq>5.00</pAliq>"
    trib_mun_final = aliq + ret if (p.estrutura or p.versao) == "1.00" else ret + aliq
    return (
        f'<DPS versao="{p.versao}"><infDPS Id="{ID_DPS}"><tpAmb>2</tpAmb><dhEmi>2026-02-10T09:00:00-03:00</dhEmi>'
        "<verAplic>sintetico-1.0</verAplic><serie>1</serie><nDPS>123</nDPS><dCompet>2026-02-10</dCompet>"
        "<tpEmit>1</tpEmit><cLocEmi>3304557</cLocEmi>"
        f"{_pessoa('prest', 'CNPJ', CNPJ_PRESTADOR, 'PRESTADOR FICTICIO LTDA', regtrib=True)}"
        f"{toma}{interm if p.intermediario else ''}"
        "<serv><locPrest><cLocPrestacao>3304557</cLocPrestacao></locPrest>"
        "<cServ><cTribNac>010101</cTribNac><cTribMun>001</cTribMun><xDescServ>Servico sintetico de teste</xDescServ>"
        "<cNBS>115090000</cNBS><cIntContrib>ABC123</cIntContrib></cServ></serv>"
        "<valores><vServPrest><vServ>1000.00</vServ></vServPrest>"
        f"{ded}<trib><tribMun><tribISSQN>1</tribISSQN>{trib_mun_final}</tribMun>"
        "<totTrib><indTotTrib>0</indTotTrib></totTrib></trib></valores></infDPS></DPS>"
    )


IBSCBS_NFSE = (
    "<IBSCBS><cLocalidadeIncid>3304557</cLocalidadeIncid><xLocalidadeIncid>Rio de Janeiro</xLocalidadeIncid>"
    "<valores><vBC>1000.00</vBC><uf><pIBSUF>0.10</pIBSUF><pAliqEfetUF>0.10</pAliqEfetUF></uf>"
    "<mun><pIBSMun>0.00</pIBSMun><pAliqEfetMun>0.00</pAliqEfetMun></mun>"
    "<fed><pCBS>0.90</pCBS><pAliqEfetCBS>0.90</pAliqEfetCBS></fed></valores>"
    "<totCIBS><vTotNF>1000.00</vTotNF><gIBS><vIBSTot>1.00</vIBSTot><gIBSUFTot><vIBSUF>1.00</vIBSUF></gIBSUFTot>"
    "<gIBSMunTot><vIBSMun>0.00</vIBSMun></gIBSMunTot></gIBS><gCBS><vCBS>9.00</vCBS></gCBS></totCIBS></IBSCBS>"
)


def nfse_xml(p: Nfse | None = None) -> str:
    """Monta uma NFS-e sintética (``<NFSe>`` com a DPS embutida) válida pelo XSD da versão pedida."""
    p = p or Nfse()
    id_nfse = "NFS" + CHAVE_NFSE
    ibscbs = IBSCBS_NFSE if p.ibscbs else ""
    return (
        f'<?xml version="1.0" encoding="UTF-8"?><NFSe xmlns="{NS}" versao="{p.versao}">'
        f'<infNFSe Id="{id_nfse}"><xLocEmi>Rio de Janeiro</xLocEmi><xLocPrestacao>Rio de Janeiro</xLocPrestacao>'
        "<nNFSe>123</nNFSe><cLocIncid>3304557</cLocIncid><xLocIncid>Rio de Janeiro</xLocIncid>"
        "<xTribNac>Servico de teste</xTribNac><xTribMun>Servico municipal de teste</xTribMun><xNBS>Servico NBS de teste</xNBS>"
        "<verAplic>sintetico-1.0</verAplic><ambGer>2</ambGer><tpEmis>1</tpEmis><procEmi>1</procEmi><cStat>100</cStat>"
        "<dhProc>2026-02-10T10:15:30-03:00</dhProc><nDFSe>123</nDFSe>"
        f"{_emitente()}"
        "<valores><vBC>1000.00</vBC><pAliqAplic>5.00</pAliqAplic><vISSQN>50.00</vISSQN><vLiq>1000.00</vLiq></valores>"
        f"{p.extra_inf_nfse}{ibscbs}{_dps(p)}</infNFSe>{ASSINATURA.format(ref=id_nfse)}</NFSe>"
    )


def mutar(xml: str, xpath: str, valor: str | None) -> str:
    """Troca o texto do primeiro nó do XPath (``None`` remove o nó) e devolve o XML serializado."""
    arvore = etree.fromstring(xml.encode("utf-8"))
    nos = arvore.xpath(xpath, namespaces=NSMAP)
    if not nos:
        raise LookupError(f"XPath sem resultado: {xpath}")
    no = nos[0]
    if valor is None:
        no.getparent().remove(no)
    else:
        no.text = valor
    return etree.tostring(arvore, encoding="unicode")


DETALHES_EVENTO = {
    "e101101": (
        "<e101101><xDesc>Cancelamento de NFS-e</xDesc><cMotivo>1</cMotivo>"
        "<xMotivo>Erro na emissao do documento fiscal</xMotivo></e101101>"
    ),
    "e105102": (
        "<e105102><xDesc>Cancelamento de NFS-e por Substituição</xDesc><cMotivo>01</cMotivo>"
        f"<chSubstituta>{CHAVE_SUBSTITUTA}</chSubstituta></e105102>"
    ),
    "e305101": (
        f"<e305101><xDesc>Cancelamento de NFS-e por Ofício</xDesc><CPFAgTrib>{CPF_AUTOR}</CPFAgTrib>"
        "<nProcAdm>123456</nProcAdm><xProcAdm>Processo administrativo sintetico</xProcAdm></e305101>"
    ),
    "e305102": (
        f"<e305102><xDesc>Bloqueio de NFS-e por Ofício</xDesc><CPFAgTrib>{CPF_AUTOR}</CPFAgTrib>"
        "<codEvento>e101101</codEvento><xMotivo>Bloqueio sintetico para teste</xMotivo></e305102>"
    ),
    "e305103": (
        f"<e305103><xDesc>Desbloqueio de NFS-e por Ofício</xDesc><CPFAgTrib>{CPF_AUTOR}</CPFAgTrib>"
        f"<idBloqOfic>{'0' * 59}</idBloqOfic></e305103>"
    ),
}


def evento_xml(tipo: str = "e101101", versao: str = "1.01", detalhe: str | None = None) -> str:
    """Monta um ``<evento>`` sintético (pedido de registro embutido) válido pelo XSD da versão pedida."""
    corpo = detalhe if detalhe is not None else DETALHES_EVENTO[tipo]
    id_pedido = "PRE" + CHAVE_NFSE + tipo.removeprefix("e")
    id_evento = id_pedido.replace("PRE", "EVT", 1) + "001"
    return (
        f'<?xml version="1.0" encoding="UTF-8"?><evento xmlns="{NS}" versao="{versao}">'
        f'<infEvento Id="{id_evento}"><verAplic>sintetico-1.0</verAplic><ambGer>2</ambGer><nSeqEvento>1</nSeqEvento>'
        "<dhProc>2026-02-11T08:00:00-03:00</dhProc><nDFSe>123</nDFSe>"
        f'<pedRegEvento versao="{versao}"><infPedReg Id="{id_pedido}"><tpAmb>2</tpAmb><verAplic>sintetico-1.0</verAplic>'
        f"<dhEvento>2026-02-11T07:30:00-03:00</dhEvento><CNPJAutor>{CNPJ_PRESTADOR}</CNPJAutor>"
        f"<chNFSe>{CHAVE_NFSE}</chNFSe>{corpo}</infPedReg></pedRegEvento></infEvento>"
        f"{ASSINATURA.format(ref=id_evento)}</evento>"
    )


def inserir(xml: str, xpath: str, trecho: str, *, depois: bool = True) -> str:
    """Insere um trecho XML (sem namespace próprio) antes ou depois do primeiro nó do XPath."""
    arvore = etree.fromstring(xml.encode("utf-8"))
    nos = arvore.xpath(xpath, namespaces=NSMAP)
    if not nos:
        raise LookupError(f"XPath sem resultado: {xpath}")
    no = nos[0]
    novo = etree.fromstring(f'<w xmlns="{NS}">{trecho}</w>'.encode())
    pai = no.getparent()
    posicao = pai.index(no) + (1 if depois else 0)
    for i, filho in enumerate(list(novo)):
        pai.insert(posicao + i, filho)
    return etree.tostring(arvore, encoding="unicode")


def mutar_atributo(xml: str, xpath: str, nome: str, valor: str) -> str:
    """Troca o valor de um atributo do primeiro nó do XPath."""
    arvore = etree.fromstring(xml.encode("utf-8"))
    nos = arvore.xpath(xpath, namespaces=NSMAP)
    if not nos:
        raise LookupError(f"XPath sem resultado: {xpath}")
    nos[0].set(nome, valor)
    return etree.tostring(arvore, encoding="unicode")
