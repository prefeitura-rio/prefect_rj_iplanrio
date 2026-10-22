"""Tasks of rj_iplanrio__sici."""

from typing import cast
from xml.etree.ElementTree import Element

from prefect import task
from toolkit.telemetry import logger
from zeep import Client

from pipelines.rj_iplanrio__sici.helpers import read_rows
from pipelines.rj_iplanrio__sici.settings import Settings


@task
def fetch_units_task(wsdl: str) -> list[dict[str, str | None]]:
    """Fetch the administrative units of the SICI tree from its SOAP API."""
    settings = Settings()
    response = cast(
        "list[Element]",
        Client(wsdl=wsdl).service.Get_Arvore_UA(
            Codigo_UA="",
            Nivel="",
            Tipo_Arvore="",
            consumidor=settings.SICI_CONSUMIDOR,
            chaveAcesso=settings.SICI_CHAVE_ACESSO.get_secret_value(),
        ),
    )
    rows = read_rows(response)
    logger.info("Units fetched: count=%d", len(rows))

    return rows
