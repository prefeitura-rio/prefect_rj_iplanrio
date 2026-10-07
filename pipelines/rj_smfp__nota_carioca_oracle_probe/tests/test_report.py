from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.format import (
    fmt_duration,
    fmt_int,
    fmt_mb_per_second,
    fmt_rows_per_second,
    key_values,
    text_table,
)
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.report_setup import format_soft
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.scaling import ScalingRun
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.report_bench import format_scaling
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.softquery import SoftRows


def test_numbers_use_thousands_separators_and_units() -> None:
    assert fmt_int(190_000_000) == "190,000,000"
    assert fmt_int(None) == "n/d"
    assert fmt_rows_per_second(12345.6) == "12,346 rows/s"
    assert fmt_mb_per_second(50_000_000, 2.0) == "25.0 MB/s"
    assert fmt_mb_per_second(1, 0.0) == "0.0 MB/s"


def test_durations_pick_a_readable_unit() -> None:
    assert fmt_duration(42) == "42 s"
    assert fmt_duration(37 * 60) == "37 min"
    assert fmt_duration(3 * 3600 + 5 * 60) == "3 h 05 min"
    assert fmt_duration(2 * 86400 + 3 * 3600) == "2 d 03 h"
    assert fmt_duration(None) == "n/d"


def test_text_table_aligns_columns() -> None:
    lines = text_table(["tabela", "linhas"], [["DPS", "190,000,000"], ["PESSOAS", "40"]]).splitlines()

    assert len({len(line) for line in lines}) == 1
    assert lines[2].endswith("190,000,000")
    assert lines[3].endswith("         40")


def test_key_values_aligns_the_separator() -> None:
    lines = key_values([("a", "1"), ("longer", "2")]).splitlines()

    assert [line.index(":") for line in lines] == [7, 7]


def test_failed_section_shows_the_note() -> None:
    note = "sem privilégio para v$version — peça à DBA: GRANT SELECT ON SYS.V_$VERSION / SELECT_CATALOG_ROLE"

    text = format_soft("Versão", SoftRows("v$version", None, note))

    assert note in text


def test_scaling_report_lists_skipped_runs_with_reason() -> None:
    runs = [
        ScalingRun(1, "completo", 1000, 10_000, 10.0, 12.0, 9.0),
        ScalingRun(4, "completo", 0, 0, 0.0, 0.0, 0.0, skipped="estimativa alta"),
    ]

    text = format_scaling("DPS", runs)

    assert "N=4 ignorado: estimativa alta" in text
    assert "100 rows/s" in text
