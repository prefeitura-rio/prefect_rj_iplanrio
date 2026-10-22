"""Helpers of rj_iplanrio__sici."""

from collections.abc import Iterable
from datetime import datetime
from xml.etree.ElementTree import Element

from pipelines.rj_iplanrio__sici.constants import SP_TZ


def read_rows(units: Iterable[Element]) -> list[dict[str, str | None]]:
    """Return one row for each unit of the XML, with the text of each child and `updated_at`.

    `updated_at` is the time of the read in São Paulo, in ISO format.
    """
    updated_at = datetime.now(SP_TZ).isoformat()

    return [{**{child.tag: child.text for child in unit}, "updated_at": updated_at} for unit in units]
