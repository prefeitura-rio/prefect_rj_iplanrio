import pytest


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("' a' || chr(10) || 'b '", "a b", id="a_newline_becomes_a_space"),
        pytest.param("'x' || chr(0) || 'y'", "xy", id="a_nul_character_is_removed"),
        pytest.param("'None'", None, id="the_text_none_is_null"),
        pytest.param("'   '", None, id="blank_text_is_null"),
        pytest.param("NULL::VARCHAR", None, id="null_stays_null"),
    ],
)
async def test_clean_text_cleans_a_text_value(scalar, value, expected):
    """Remove NUL, turn line breaks into spaces, trim, and turn empty into NULL."""
    assert await scalar(f"clean_text({value})") == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("2024-03-05", "2024-03-05", id="iso"),
        pytest.param("05/03/2024", "2024-03-05", id="day_first"),
        pytest.param("20240305", "2024-03-05", id="compact"),
        pytest.param(
            "2024-03-05 10:30:00", "2024-03-05", id="a_timestamp_keeps_its_date"
        ),
        pytest.param("lixo", None, id="garbage_is_null"),
    ],
)
async def test_parse_date_reads_the_formats_of_the_pipelines(scalar, text, expected):
    """Read a date in any format that the pipelines use, and give NULL for garbage."""
    value = await scalar(f"parse_date('{text}')::VARCHAR")

    assert value == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param(
            "2024-03-05 10:30:00", "2024-03-05 10:30:00", id="iso_with_a_space"
        ),
        pytest.param("2024-03-05T10:30:00", "2024-03-05 10:30:00", id="iso_with_a_t"),
        pytest.param("05/03/2024 10:30:00", "2024-03-05 10:30:00", id="day_first"),
        pytest.param(
            "05/03/2024 - 10:30:00", "2024-03-05 10:30:00", id="day_first_with_a_dash"
        ),
        pytest.param("2024-03-05 10:30", "2024-03-05 10:30:00", id="no_seconds"),
        pytest.param("202403051030", "2024-03-05 10:30:00", id="compact"),
        pytest.param("lixo", None, id="garbage_is_null"),
    ],
)
async def test_parse_timestamp_reads_the_formats_of_the_pipelines(
    scalar, text, expected
):
    """Read a timestamp in any format that the pipelines use."""
    value = await scalar(f"parse_timestamp('{text}')::VARCHAR")

    assert value == expected


@pytest.mark.parametrize(
    ("macro", "expected"),
    [
        pytest.param("ano_particao", 2024, id="year"),
        pytest.param("mes_particao", 3, id="month"),
        pytest.param("data_particao", "2024-03-05", id="date"),
    ],
)
async def test_the_partition_macros_derive_the_columns_of_the_folders(
    scalar, macro, expected
):
    """Derive the year, month and date of a partition from a date text."""
    value = await scalar(f"{macro}('05/03/2024')")

    assert str(value) == str(expected)
