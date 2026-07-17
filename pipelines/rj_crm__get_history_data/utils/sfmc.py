# -*- coding: utf-8 -*-
"""
Funções auxiliares (não-task) para integração SOAP/REST com o Salesforce
Marketing Cloud (SFMC).
"""

import re
from typing import Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

_SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
_ET_NS = "http://exacttarget.com/wsdl/partnerAPI"


def normalize_field_name(name: str) -> str:
    """Reduz espaços, underscores e hífens a um único espaço para comparar nomes de campo."""
    return re.sub(r"[\s_-]+", " ", name.strip().lower())


def build_soap_envelope(access_token: str, continue_request_id: Optional[str] = None) -> bytes:
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


def parse_soap_response(xml_text: str) -> Tuple[str, str, List[Dict[str, str]]]:
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


def build_field_types_soap_envelope(access_token: str, customer_key: str) -> bytes:
    """Monta o envelope SOAP para listagem do schema (campos) de uma DataExtension."""
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
</soapenv:Envelope>"""
    return envelope.encode("utf-8")


def parse_field_types_soap_response(xml_text: str) -> List[Dict[str, str]]:
    """Parseia a resposta SOAP de schema (campos) de uma DataExtension."""
    root = ET.fromstring(xml_text)
    body = root.find(f"{{{_SOAP_NS}}}Body")
    response_msg = body.find(f"{{{_ET_NS}}}RetrieveResponseMsg")

    fields = []
    for result in response_msg.findall(f"{{{_ET_NS}}}Results"):
        fields.append({
            "name": result.findtext(f"{{{_ET_NS}}}Name", default=""),
            "type": result.findtext(f"{{{_ET_NS}}}FieldType", default=""),
            "is_pk": result.findtext(f"{{{_ET_NS}}}IsPrimaryKey", default="false"),
        })

    return fields
