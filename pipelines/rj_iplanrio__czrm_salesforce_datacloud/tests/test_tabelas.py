"""Tests for helpers/tabelas.py — table configuration loader."""

import tempfile
from pathlib import Path

import pytest

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.tabelas import (
    TABLES,
    Table,
    load_tables,
)

VALID_YAML = """
- nome: account
  grupo: Cadastro
  dlo:
    qa: Account_QA__dll
    staging: Account_Home__dll
    prod: Account_Home__dll
  primary_key: id
  order_by: Id__c
"""


class TestLoadTables:
    def _write(self, content: str) -> Path:
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8")
        f.write(content)
        f.close()
        return Path(f.name)

    def test_valid_yaml_loads(self) -> None:
        tables = load_tables(self._write(VALID_YAML))
        assert len(tables) == 1
        assert tables[0].nome == "account"

    def test_raises_on_missing_origin(self) -> None:
        yaml = """
- nome: account
  grupo: Cadastro
  dlo:
    qa: X__dll
    prod: X__dll
"""
        with pytest.raises(ValueError, match="missing origin"):
            load_tables(self._write(yaml))

    def test_raises_on_unknown_field(self) -> None:
        yaml = """
- nome: account
  grupo: Cadastro
  dlo:
    qa: X__dll
    staging: X__dll
    prod: X__dll
  unknown_field: bad
"""
        with pytest.raises(ValueError, match="unknown field"):
            load_tables(self._write(yaml))

    def test_raises_on_duplicate_name(self) -> None:
        yaml = VALID_YAML + VALID_YAML
        with pytest.raises(ValueError, match="duplicate"):
            load_tables(self._write(yaml))

    def test_raises_on_invalid_primary_key(self) -> None:
        yaml = """
- nome: account
  grupo: Cadastro
  dlo:
    qa: X__dll
    staging: X__dll
    prod: X__dll
  primary_key: "INVALID KEY"
"""
        with pytest.raises(ValueError, match="snake_case"):
            load_tables(self._write(yaml))

    def test_raises_on_invalid_order_by_identifier(self) -> None:
        yaml = """
- nome: account
  grupo: Cadastro
  dlo:
    qa: X__dll
    staging: X__dll
    prod: X__dll
  order_by: "Id; DROP TABLE"
"""
        with pytest.raises(ValueError, match="identifier"):
            load_tables(self._write(yaml))


class TestTable:
    TABLE = Table(
        nome="account",
        grupo="Cadastro",
        dlo={"qa": "Account_QA__dll", "staging": "Account_Home__dll", "prod": "Account_Home__dll"},
    )

    def test_staging_name(self) -> None:
        assert self.TABLE.staging == "account_staging"

    def test_dlo_for_valid_origin(self) -> None:
        assert self.TABLE.dlo_for("prod") == "Account_Home__dll"

    def test_dlo_for_invalid_origin_raises(self) -> None:
        with pytest.raises(ValueError, match="no DLO"):
            self.TABLE.dlo_for("unknown")


class TestTABLES:
    def test_tables_loaded(self) -> None:
        assert len(TABLES) > 0

    def test_all_have_three_origins(self) -> None:
        for table in TABLES:
            assert set(table.dlo.keys()) == {"qa", "staging", "prod"}
