"""The SOAP messages of SFMC, and how to read their answers."""

from xml.etree.ElementTree import Element

from defusedxml.ElementTree import fromstring

from pipelines.rj_crm__get_history_data.constants import PARTNER_NS, SOAP_NS
from pipelines.rj_crm__get_history_data.types import Extension, Field

ENVELOPE = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope
    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
    xmlns="http://exacttarget.com/wsdl/partnerAPI">
    <soapenv:Header>
        <fueloauth xmlns="http://exacttarget.com/wsdl/partnerAPI">{token}</fueloauth>
    </soapenv:Header>
    <soapenv:Body>
        <RetrieveRequestMsg>
            <RetrieveRequest>
                {request}
            </RetrieveRequest>
        </RetrieveRequestMsg>
    </soapenv:Body>
</soapenv:Envelope>"""
LIST_REQUEST = (
    "<ObjectType>DataExtension</ObjectType><Properties>Name</Properties>"
    "<Properties>CustomerKey</Properties><Properties>ObjectID</Properties>"
)
FIELDS_REQUEST = (
    "<ObjectType>DataExtensionField</ObjectType><Properties>Name</Properties><Properties>FieldType</Properties>"
    "<Properties>IsRequired</Properties><Properties>IsPrimaryKey</Properties>"
    '<Filter xsi:type="SimpleFilterPart"><Property>DataExtension.CustomerKey</Property>'
    "<SimpleOperator>equals</SimpleOperator><Value>{key}</Value></Filter>"
)


def build_list_envelope(token: str, continue_request_id: str | None = None) -> bytes:
    """Return the message that lists the Data Extensions, or that asks for the next page of a list."""
    request = f"<ContinueRequest>{continue_request_id}</ContinueRequest>" if continue_request_id else LIST_REQUEST

    return ENVELOPE.format(token=token, request=request).encode()


def build_fields_envelope(token: str, customer_key: str) -> bytes:
    """Return the message that lists the columns of a Data Extension."""
    return ENVELOPE.format(token=token, request=FIELDS_REQUEST.format(key=customer_key)).encode()


def find_response(xml_text: str) -> Element:
    """Return the `RetrieveResponseMsg` of an answer. Raise `ValueError` if the answer does not have one."""
    body = fromstring(xml_text).find(f"{{{SOAP_NS}}}Body")
    response = body.find(f"{{{PARTNER_NS}}}RetrieveResponseMsg") if body is not None else None

    if response is None:
        raise ValueError("The SOAP answer has no RetrieveResponseMsg")

    return response


def parse_extensions(xml_text: str) -> tuple[str, str, list[Extension]]:
    """Read the answer that lists the Data Extensions: the overall status, the id of the request and the extensions."""
    response = find_response(xml_text)
    extensions = [
        Extension(
            result.findtext(f"{{{PARTNER_NS}}}Name", default=""),
            result.findtext(f"{{{PARTNER_NS}}}CustomerKey", default=""),
        )
        for result in response.findall(f"{{{PARTNER_NS}}}Results")
    ]

    return (
        response.findtext(f"{{{PARTNER_NS}}}OverallStatus", default=""),
        response.findtext(f"{{{PARTNER_NS}}}RequestID", default=""),
        extensions,
    )


def parse_fields(xml_text: str) -> list[Field]:
    """Read the answer that lists the columns of a Data Extension."""
    return [
        Field(
            result.findtext(f"{{{PARTNER_NS}}}Name", default=""),
            result.findtext(f"{{{PARTNER_NS}}}FieldType", default=""),
            result.findtext(f"{{{PARTNER_NS}}}IsPrimaryKey", default="false").lower() == "true",
        )
        for result in find_response(xml_text).findall(f"{{{PARTNER_NS}}}Results")
    ]
