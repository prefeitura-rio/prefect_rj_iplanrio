"""Constants of rj_crm__get_history_data."""

from typing import Final

DE_SUFFIX: Final = "historico"
"""The suffix of the name of a Data Extension that holds history."""
PAGE_SIZE: Final = 2500
"""The most rows that the REST API of SFMC gives in a page."""
PROJECT_ID: Final = "rj-crm-registry"
DATASET_ID: Final = "brutos_salesforce_staging"
TMP_TABLE_ID: Final = "historico_sfmc_tmp"
FINAL_TABLE_ID: Final = "historico_sfmc"
DEFAULT_WINDOW_DAYS: Final = 30
"""The days of `entrada_data` that each run takes."""
MERGE_KEYS: Final = ("de_nome", "telefone", "entrada_data")
SOAP_NS: Final = "http://schemas.xmlsoap.org/soap/envelope/"
PARTNER_NS: Final = "http://exacttarget.com/wsdl/partnerAPI"
SOAP_HEADERS: Final = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": "Retrieve"}
DATE_FORMATS: Final = (
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y",
)
"""The SFMC date formats after the ISO format: month first, then day first."""
HTTP_TIMEOUT: Final = 60
