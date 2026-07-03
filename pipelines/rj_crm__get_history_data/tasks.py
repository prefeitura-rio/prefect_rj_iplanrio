# -*- coding: utf-8 -*-
"""
Tasks para pipeline CRM Get History Data (SFMC)
Extrai dados de Data Extensions com sufixo 'historico' do Salesforce Marketing Cloud
"""

import os
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple

import requests
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_crm__get_history_data.constants import GetHistoryDataConstants

_SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
_ET_NS = "http://exacttarget.com/wsdl/partnerAPI"


@task
def authenticate_sfmc() -> str:
    """
    Autentica com Salesforce Marketing Cloud via OAuth2 e retorna o access token.
    POST {SFMC_AUTH_URI}/v2/token
    """
    auth_uri = os.getenv("API_SFMC_AUTH_URL", "")
    client_id = os.getenv("API_SFMC_CLIENT_ID", "")
    client_secret = os.getenv("API_SFMC_CLIENT_SECRET", "")

    missing = [
        name for name, val in {
            "API_SFMC_AUTH_URL": auth_uri,
            "API_SFMC_CLIENT_ID": client_id,
            "API_SFMC_CLIENT_SECRET": client_secret,
        }.items()
        if not val
    ]
    if missing:
        raise ValueError(f"Variáveis de ambiente SFMC ausentes: {missing}")

    url = f"{auth_uri.rstrip('/')}/v2/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    log(f"Autenticando no SFMC via {url}")
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()

    data = response.json()
    access_token = data.get("access_token")
    if not access_token:
        raise ValueError(f"access_token não encontrado na resposta SFMC: {list(data.keys())}")

    expires_in = data.get("expires_in", GetHistoryDataConstants.TOKEN_CACHE_SECONDS.value)
    log(f"Autenticação SFMC bem-sucedida. Token válido por {expires_in}s")
    return access_token


def _build_soap_envelope(access_token: str, continue_request_id: Optional[str] = None) -> bytes:
    """Monta o envelope SOAP para listagem de DataExtensions."""
    if continue_request_id:
        body_inner = f"""
        <RetrieveRequestMsg>
            <RetrieveRequest>
                <ContinueRequest>{continue_request_id}</ContinueRequest>
            </RetrieveRequest>
        </RetrieveRequestMsg>
        """
    else:
        body_inner = """
        <RetrieveRequestMsg>
            <RetrieveRequest>
                <ObjectType>DataExtension</ObjectType>
                <Properties>Name</Properties>
                <Properties>CustomerKey</Properties>
                <Properties>ObjectID</Properties>
            </RetrieveRequest>
        </RetrieveRequestMsg>
        """

    envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope
    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
    xmlns="http://exacttarget.com/wsdl/partnerAPI">
    <soapenv:Header>
        <fueloauth xmlns="http://exacttarget.com/wsdl/partnerAPI">{access_token}</fueloauth>
    </soapenv:Header>
    <soapenv:Body>
        {body_inner}
    </soapenv:Body>
</soapenv:Envelope>"""
    return envelope.encode("utf-8")


def _parse_soap_response(xml_text: str) -> Tuple[str, str, List[Dict[str, str]]]:
    """
    Parseia a resposta SOAP de listagem de DataExtensions.
    Retorna (overall_status, request_id, lista de DEs)
    """
    root = ET.fromstring(xml_text)
    body = root.find(f"{{{_SOAP_NS}}}Body")
    response_msg = body.find(f"{{{_ET_NS}}}RetrieveResponseMsg")

    overall_status = response_msg.findtext(f"{{{_ET_NS}}}OverallStatus", default="")
    request_id = response_msg.findtext(f"{{{_ET_NS}}}RequestID", default="")

    results = []
    for result in response_msg.findall(f"{{{_ET_NS}}}Results"):
        name = result.findtext(f"{{{_ET_NS}}}Name", default="")
        customer_key = result.findtext(f"{{{_ET_NS}}}CustomerKey", default="")
        object_id = result.findtext(f"{{{_ET_NS}}}ObjectID", default="")
        results.append({
            "name": name,
            "external_key": customer_key,
            "object_id": object_id,
        })

    return overall_status, request_id, results


@task
def list_data_extensions_historico(access_token: str, soap_uri: str) -> List[Dict[str, str]]:
    """
    Lista todas as Data Extensions com nome terminando em 'historico' via SOAP API.
    Trata paginação SOAP via status MoreDataAvailable.

    Args:
        access_token: Token OAuth2 do SFMC
        soap_uri: URI base do endpoint SOAP (ex: https://mcXXX.soap.marketingcloudapis.com)

    Returns:
        Lista de dicts com {name, external_key, object_id}
    """
    endpoint = f"{soap_uri.rstrip('/')}/Service.asmx"
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "Retrieve",
    }

    all_des: List[Dict[str, str]] = []
    continue_request_id: Optional[str] = None
    page = 0

    while True:
        log(f"Buscando DataExtensions - página SOAP {page}")
        envelope = _build_soap_envelope(access_token, continue_request_id)
        response = requests.post(endpoint, data=envelope, headers=headers, timeout=60)
        response.raise_for_status()

        overall_status, request_id, results = _parse_soap_response(response.text)
        log(f"Página SOAP {page}: status={overall_status}, DEs retornadas={len(results)}")
        all_des.extend(results)

        if overall_status == "MoreDataAvailable":
            continue_request_id = request_id
            page += 1
        elif overall_status in ("OK", ""):
            break
        else:
            raise RuntimeError(f"SOAP API retornou status inesperado: {overall_status}")

    suffix = GetHistoryDataConstants.DE_SUFFIX.value
    historico_des = [de for de in all_des if de["name"].lower().endswith(suffix)]

    log(f"Total de DEs encontradas: {len(all_des)}, com sufixo '{suffix}': {len(historico_des)}")
    for de in historico_des:
        log(f"  -> {de['name']} (key={de['external_key']}, id={de['object_id']})")

    return historico_des


@task
def fetch_data_extension_data(
    access_token: str,
    external_key: str,
    de_name: str,
    rest_uri: str,
) -> List[Dict[str, Any]]:
    """
    Extrai todos os registros de uma Data Extension via REST API com paginação.
    GET {rest_uri}/data/v1/customobjectdata/key/{external_key}/rowset?$page={page}

    Args:
        access_token: Token OAuth2 do SFMC
        external_key: CustomerKey da DE
        de_name: Nome da DE (para logging)
        rest_uri: URI base do endpoint REST (ex: https://mcXXX.rest.marketingcloudapis.com)

    Returns:
        Lista de dicts com todos os registros da DE
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    base_url = f"{rest_uri.rstrip('/')}/data/v1/customobjectdata/key/{external_key}/rowset"

    all_rows: List[Dict[str, Any]] = []
    page = 1

    log(f"Iniciando extração de '{de_name}' (key={external_key})")

    while True:
        response = requests.get(base_url, headers=headers, params={"$page": page}, timeout=60)
        response.raise_for_status()

        data = response.json()
        page_count = data.get("pageCount", 1)
        items = data.get("items", [])

        log(f"  [{de_name}] Página {page}/{page_count}: {len(items)} registros")
        all_rows.extend(items)

        if page >= page_count:
            break
        page += 1

    log(f"  [{de_name}] Extração concluída: {len(all_rows)} registros no total")
    return all_rows


@task
def fetch_de_field_types(
    access_token: str,
    customer_key: str,
    de_name: str,
    soap_uri: str,
) -> List[Dict[str, str]]:
    """
    Retorna o schema (nome + tipo + pk) de uma Data Extension via SOAP API.
    Usa ObjectType=DataExtensionField filtrado por DataExtension.CustomerKey.
    """
    endpoint = f"{soap_uri.rstrip('/')}/Service.asmx"
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "Retrieve",
    }

    envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope
    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
    xmlns="http://exacttarget.com/wsdl/partnerAPI">
    <soapenv:Header>
        <fueloauth xmlns="http://exacttarget.com/wsdl/partnerAPI">{access_token}</fueloauth>
    </soapenv:Header>
    <soapenv:Body>
        <RetrieveRequestMsg>
            <RetrieveRequest>
                <ObjectType>DataExtensionField</ObjectType>
                <Properties>Name</Properties>
                <Properties>FieldType</Properties>
                <Properties>IsRequired</Properties>
                <Properties>IsPrimaryKey</Properties>
                <Filter xsi:type="SimpleFilterPart">
                    <Property>DataExtension.CustomerKey</Property>
                    <SimpleOperator>equals</SimpleOperator>
                    <Value>{customer_key}</Value>
                </Filter>
            </RetrieveRequest>
        </RetrieveRequestMsg>
    </soapenv:Body>
</soapenv:Envelope>""".encode("utf-8")

    response = requests.post(endpoint, data=envelope, headers=headers, timeout=60)
    response.raise_for_status()

    root = ET.fromstring(response.text)
    body = root.find(f"{{{_SOAP_NS}}}Body")
    response_msg = body.find(f"{{{_ET_NS}}}RetrieveResponseMsg")

    fields = []
    for result in response_msg.findall(f"{{{_ET_NS}}}Results"):
        fields.append({
            "name": result.findtext(f"{{{_ET_NS}}}Name", default=""),
            "type": result.findtext(f"{{{_ET_NS}}}FieldType", default=""),
            "is_pk": result.findtext(f"{{{_ET_NS}}}IsPrimaryKey", default="false"),
        })

    log(f"  [{de_name}] Schema: {len(fields)} campos | "
        f"phone={next((f['name'] for f in fields if f['type'].lower() == 'phone'), None)} | "
        f"pk={next((f['name'] for f in fields if f['is_pk'].lower() == 'true'), None)}")
    return fields


@task
def build_historico_dataframe(results: List[Dict[str, Any]]) -> None:
    """
    Constrói e loga um DataFrame com uma linha por registro de cada DE.

    Colunas:
        de_name  - nome da Data Extension
        telefone - valor da coluna de tipo Phone
        pk_de    - valor da coluna que é Primary Key
        data     - todos os campos do registro em JSON
    """
    import json

    import pandas as pd

    rows = []
    for result in results:
        if result.get("status") != "success":
            continue

        de_name = result["de_name"]
        dados = result.get("dados", [])
        field_types = result.get("tipos", [])

        phone_col = next((f["name"] for f in field_types if f["type"].lower() == "phone"), None)
        pk_col = next((f["name"] for f in field_types if f["is_pk"].lower() == "true"), None)

        for item in dados:
            keys = item.get("keys", item)
            rows.append({
                "de_name": de_name,
                "telefone": keys.get(phone_col) if phone_col else None,
                "pk_de": keys.get(pk_col) if pk_col else None,
                "data": json.dumps(keys, ensure_ascii=False),
            })

    df = pd.DataFrame(rows, columns=["de_name", "telefone", "pk_de", "data"])

    log("=" * 60)
    log("DATAFRAME HISTORICO SFMC")
    log(f"Shape: {df.shape[0]} linhas x {df.shape[1]} colunas")
    log("=" * 60)
    log(f"\n{df.to_string()}")


@task
def process_historico_extraction(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Agrega resultados da extração e loga resumo estruturado.

    Args:
        results: Lista de dicts com resultado por DE
                 (campos: de_name, external_key, status, total_rows, sample, error)

    Returns:
        Dict com metadados: des_sucesso, des_erro, total_registros, detalhes
    """
    des_sucesso = [r for r in results if r.get("status") == "success"]
    des_erro = [r for r in results if r.get("status") == "error"]
    total_registros = sum(r.get("total_rows", 0) for r in des_sucesso)

    log("=" * 60)
    log("RESUMO DA EXTRACAO DE HISTORICOS SFMC")
    log("=" * 60)
    log(f"DEs processadas com sucesso : {len(des_sucesso)}")
    log(f"DEs com erro                : {len(des_erro)}")
    log(f"Total de registros extraidos: {total_registros}")

    if des_sucesso:
        log("\nAmostras (1 linha por DE):")
        for r in des_sucesso:
            log(f"  [{r['de_name']}] total={r['total_rows']} | amostra={r.get('sample')}")

    if des_erro:
        log("\nErros detalhados:")
        for r in des_erro:
            log(f"  [{r['de_name']}] ERRO: {r.get('error')}", level="error")

    log("=" * 60)

    return {
        "des_sucesso": len(des_sucesso),
        "des_erro": len(des_erro),
        "total_registros": total_registros,
        "detalhes": results,
    }
