"""The KML of a QMD."""

from collections.abc import Iterator
from io import BytesIO
from json import dumps
from warnings import catch_warnings, simplefilter

from fastkml import KML, Data, Document, Folder, Placemark
from toolkit.telemetry import logger

from pipelines.rj_segur__forca_municipal.types import Row


def iter_placemarks(node: KML | Document | Folder, folder: str | None = None) -> Iterator[tuple[Placemark, str | None]]:
    """Return the placemarks at any depth of a KML node, each with the name of the folder around it."""
    for feature in node.features:
        if isinstance(feature, Placemark):
            yield feature, folder
        elif isinstance(feature, Document | Folder):
            yield from iter_placemarks(feature, feature.name or folder)


def parse_placemarks(text: str, qmd_id: str) -> list[Row]:
    """Return a row for each placemark of the KML of a QMD, or no row if the KML has no document or is bad.

    A row has the id of the QMD, the folder, the name, the description, the geometry as
    WKT with its type, and the extended data as JSON.
    """
    try:
        with catch_warnings():
            simplefilter("ignore")
            kml = KML.parse(BytesIO(text.encode()), strict=False)
    except Exception as error:
        logger.warning("The KML does not parse: qmd=%s error=%s", qmd_id, error)
        return []

    if not kml.features:
        logger.warning("The KML has no document: qmd=%s", qmd_id)
        return []

    rows: list[Row] = []

    for placemark, folder in iter_placemarks(
        kml.features[0] if isinstance(kml.features[0], Document | Folder) else kml
    ):
        geometry = placemark.geometry
        extended = {
            element.name: element.value
            for element in (placemark.extended_data.elements if placemark.extended_data else [])
            if isinstance(element, Data) and element.name
        }
        rows.append(
            {
                "qmd_id": qmd_id,
                "kml_folder": folder,
                "name": placemark.name,
                "description": placemark.description,
                "geometry_wkt": geometry.wkt if geometry else None,
                "geometry_type": geometry.geom_type if geometry else None,
                "extended_data": dumps(extended, ensure_ascii=False) if extended else None,
            }
        )

    return rows
