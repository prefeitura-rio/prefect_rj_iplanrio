from pathlib import Path

import duckdb
import pytest

from toolkit.dump_url import csv_scan, download_sheet, drive_file_id


def test_drive_file_id():
    assert (
        drive_file_id("https://drive.google.com/file/d/abc123/view?usp=sharing")
        == "abc123"
    )


@pytest.mark.parametrize("url", ["https://example.com/file/d/x", "abc"])
def test_drive_file_id_rejects_other_urls(url):
    with pytest.raises(ValueError, match="Google Drive URL"):
        drive_file_id(url)


def test_download_sheet_rejects_other_urls(tmp_path):
    with pytest.raises(ValueError, match="Google Sheets URL"):
        download_sheet("https://example.com/x", tmp_path / "a.csv")


@pytest.mark.parametrize(
    ("content", "options", "expected"),
    [
        pytest.param("a;b\n1;x\n", {"separator": ";"}, [(1, "x")], id="semicolon"),
        pytest.param(
            "a,b\n1,x\n2\n3,z\n",
            {"ignore_errors": True},
            [(1, "x"), (3, "z")],
            id="skips_bad_lines",
        ),
    ],
)
def test_csv_scan_reads_the_file(tmp_path: Path, content, options, expected):
    path = tmp_path / "data.csv"
    path.write_text(content)
    sql = f"SELECT * FROM {csv_scan(path, **options).as_string(None)}"
    assert duckdb.sql(sql).fetchall() == expected
