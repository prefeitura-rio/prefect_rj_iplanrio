from xml.etree.ElementTree import Element, SubElement

from pipelines.rj_iplanrio__sici.helpers import read_rows


def test_a_row_is_the_text_of_each_child_of_a_unit_and_the_time_of_the_read() -> None:
    """Read one row for each unit, with `updated_at` in São Paulo."""
    root = Element("Arvore")
    first = SubElement(root, "UA")
    SubElement(first, "Codigo").text = "10"
    SubElement(first, "Nome").text = None
    second = SubElement(root, "UA")
    SubElement(second, "Codigo").text = "20"
    SubElement(second, "Nome").text = "Secretaria"

    rows = read_rows(root)

    assert [{key: value for key, value in row.items() if key != "updated_at"} for row in rows] == [
        {"Codigo": "10", "Nome": None},
        {"Codigo": "20", "Nome": "Secretaria"},
    ]
    assert rows[0]["updated_at"] == rows[1]["updated_at"]
    assert str(rows[0]["updated_at"]).endswith("-03:00")
