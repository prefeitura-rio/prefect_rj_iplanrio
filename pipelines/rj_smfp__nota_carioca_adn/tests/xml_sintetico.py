"""XML sintético de NFS-e e de evento no leiaute nacional v1.01, sem nenhum dado real.

Cada documento é determinístico em função do NSU. ``nfse_xml`` e ``evento_xml`` foram validados com lxml contra
``NFSe_v1.01.xsd`` e ``evento_v1.01.xsd`` (zip público ``nfse-esquemas_xsd-v1-01-20260209``). O único ajuste foi numa
cópia local do XSD: o padrão de ``TSSerieDPS`` (``^0{0,4}\\d{1,5}$``) traz ``^`` e ``$`` literais, que o libxml2 trata
como caracteres; sem eles, a série ``00001`` valida. CNPJ, nomes e endereços são inventados e a assinatura é fictícia.
"""

from datetime import UTC, datetime, timedelta

NAMESPACE = "http://www.sped.fazenda.gov.br/nfse"
MUNICIPIO_RIO = "3304557"
CNPJ_PRESTADOR_FICTICIO = "11111111000111"
CNPJ_TOMADOR_FICTICIO = "22222222000122"
BASE_GERACAO = datetime(2026, 1, 5, 12, 0, 0, tzinfo=UTC)
SEGUNDOS_ENTRE_NSUS = 7


def chave_acesso(nsu: int) -> str:
    """Chave de acesso fictícia de 50 dígitos: município, ambiente, tipo, CNPJ, número, AAMM, aleatório e DV."""
    corpo = f"{MUNICIPIO_RIO}1" + "2" + CNPJ_PRESTADOR_FICTICIO + f"{nsu:013d}" + "2601" + f"{nsu % 10**9:09d}"
    return corpo + str(sum(map(int, corpo)) % 10)


def data_hora_geracao(nsu: int) -> datetime:
    """Data e hora de geração fictícias, crescentes com o NSU."""
    return BASE_GERACAO + timedelta(seconds=nsu * SEGUNDOS_ENTRE_NSUS)


def _carimbo(momento: datetime) -> str:
    return momento.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S") + "+00:00"


def _assinatura(referencia: str) -> str:
    """Bloco ``ds:Signature`` válido no esquema e sem validade criptográfica (valores fictícios)."""
    return (
        '<Signature xmlns="http://www.w3.org/2000/09/xmldsig#"><SignedInfo>'
        '<CanonicalizationMethod Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"/>'
        '<SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"/>'
        f'<Reference URI="#{referencia}"><Transforms>'
        '<Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>'
        '<Transform Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"/></Transforms>'
        '<DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>'
        "<DigestValue>U0lOVEVUSUNP</DigestValue></Reference></SignedInfo>"
        "<SignatureValue>U0lOVEVUSUNP</SignatureValue>"
        "<KeyInfo><X509Data><X509Certificate>U0lOVEVUSUNP</X509Certificate></X509Data></KeyInfo></Signature>"
    )


def nfse_xml(nsu: int) -> str:
    """NFS-e sintética (``NFSe`` v1.01) com o número igual ao NSU."""
    momento = _carimbo(data_hora_geracao(nsu))
    id_dps = f"DPS{MUNICIPIO_RIO}2{CNPJ_PRESTADOR_FICTICIO}00001{nsu:015d}"
    return (
        f'<?xml version="1.0" encoding="UTF-8"?><NFSe xmlns="{NAMESPACE}" versao="1.01">'
        f'<infNFSe Id="NFS{chave_acesso(nsu)}">'
        "<xLocEmi>Rio de Janeiro</xLocEmi><xLocPrestacao>Rio de Janeiro</xLocPrestacao>"
        f"<nNFSe>{nsu}</nNFSe><cLocIncid>{MUNICIPIO_RIO}</cLocIncid><xLocIncid>Rio de Janeiro</xLocIncid>"
        "<xTribNac>Servico sintetico de teste</xTribNac><verAplic>SINTETICO_1.0</verAplic>"
        f"<ambGer>2</ambGer><tpEmis>1</tpEmis><procEmi>1</procEmi><cStat>100</cStat><dhProc>{momento}</dhProc>"
        f"<nDFSe>{nsu}</nDFSe>"
        f"<emit><CNPJ>{CNPJ_PRESTADOR_FICTICIO}</CNPJ><IM>123456</IM><xNome>PRESTADOR SINTETICO LTDA</xNome>"
        "<enderNac><xLgr>RUA SINTETICA</xLgr><nro>100</nro><xBairro>CENTRO</xBairro>"
        f"<cMun>{MUNICIPIO_RIO}</cMun><UF>RJ</UF><CEP>20000000</CEP></enderNac></emit>"
        "<valores><vBC>100.00</vBC><pAliqAplic>5.00</pAliqAplic><vISSQN>5.00</vISSQN><vLiq>95.00</vLiq></valores>"
        f'<DPS versao="1.01"><infDPS Id="{id_dps}"><tpAmb>2</tpAmb><dhEmi>{momento}</dhEmi>'
        f"<verAplic>SINTETICO_1.0</verAplic><serie>00001</serie><nDPS>{nsu}</nDPS><dCompet>2026-01-05</dCompet>"
        f"<tpEmit>1</tpEmit><cLocEmi>{MUNICIPIO_RIO}</cLocEmi>"
        f"<prest><CNPJ>{CNPJ_PRESTADOR_FICTICIO}</CNPJ><regTrib><opSimpNac>1</opSimpNac><regEspTrib>0</regEspTrib>"
        "</regTrib></prest>"
        f"<toma><CNPJ>{CNPJ_TOMADOR_FICTICIO}</CNPJ><xNome>TOMADOR SINTETICO SA</xNome></toma>"
        f"<serv><locPrest><cLocPrestacao>{MUNICIPIO_RIO}</cLocPrestacao></locPrest>"
        "<cServ><cTribNac>010101</cTribNac><xDescServ>Servico sintetico de teste</xDescServ></cServ></serv>"
        "<valores><vServPrest><vServ>100.00</vServ></vServPrest><trib><tribMun><tribISSQN>1</tribISSQN>"
        "<tpRetISSQN>1</tpRetISSQN></tribMun><totTrib><indTotTrib>0</indTotTrib></totTrib></trib></valores>"
        "</infDPS></DPS></infNFSe>" + _assinatura(f"NFS{chave_acesso(nsu)}") + "</NFSe>"
    )


def evento_xml(nsu: int) -> str:
    """Evento sintético de cancelamento (código 101101) ligado à NFS-e de mesmo NSU."""
    momento = _carimbo(data_hora_geracao(nsu))
    chave = chave_acesso(nsu)
    return (
        f'<?xml version="1.0" encoding="UTF-8"?><evento xmlns="{NAMESPACE}" versao="1.01">'
        f'<infEvento Id="EVT{chave}101101001"><verAplic>SINTETICO_1.0</verAplic><ambGer>2</ambGer>'
        f"<nSeqEvento>1</nSeqEvento><dhProc>{momento}</dhProc><nDFSe>{nsu}</nDFSe>"
        f'<pedRegEvento versao="1.01"><infPedReg Id="PRE{chave}101101"><tpAmb>2</tpAmb>'
        f"<verAplic>SINTETICO_1.0</verAplic><dhEvento>{momento}</dhEvento>"
        f"<CNPJAutor>{CNPJ_PRESTADOR_FICTICIO}</CNPJAutor><chNFSe>{chave}</chNFSe>"
        "<e101101><xDesc>Cancelamento de NFS-e</xDesc><cMotivo>1</cMotivo>"
        "<xMotivo>Cancelamento sintetico de teste</xMotivo></e101101></infPedReg></pedRegEvento></infEvento>"
        + _assinatura(f"EVT{chave}101101001")
        + "</evento>"
    )


def tipo_documento(nsu: int) -> str:
    """``NFSE`` por padrão e ``EVENTO`` em cada décimo NSU."""
    return "EVENTO" if nsu % 10 == 0 else "NFSE"


def documento_xml(nsu: int) -> str:
    """XML do documento do NSU, conforme ``tipo_documento``."""
    return evento_xml(nsu) if tipo_documento(nsu) == "EVENTO" else nfse_xml(nsu)
