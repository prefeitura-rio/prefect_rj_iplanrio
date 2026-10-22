import pytest
from pymongoarrow.api import Schema
from pymongoarrow.types import ObjectIdType

from toolkit.executor.engines.base import Engine, load_engines
from toolkit.executor.engines.couchdb import CouchDBEngine
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.engines.mongodb import MongoDBEngine
from toolkit.executor.engines.oracle import OracleEngine
from toolkit.executor.requests import UnsupportedRequestError
from toolkit.executor.sources.backends.arcgis import ArcGISSource
from toolkit.executor.sources.backends.couchdb import CouchDBSource
from toolkit.executor.sources.backends.files import FilesSource
from toolkit.executor.sources.backends.gsheets import GSheetsSource
from toolkit.executor.sources.backends.mongodb import MongoDBSource
from toolkit.executor.sources.backends.mssql import MSSQLSource
from toolkit.executor.sources.backends.mysql import MySQLSource
from toolkit.executor.sources.backends.oracle import OracleSource
from toolkit.executor.sources.backends.postgres import PostgresSource
from toolkit.executor.sources.protocols import DBConn
from toolkit.types import Chunks

CONN = DBConn("h", 1, "d", "u", "p")
SCHEMA = Schema({"_id": ObjectIdType()})


@pytest.mark.parametrize(
    ("source", "engine"),
    [
        pytest.param(
            PostgresSource(CONN), DuckDBEngine, id="postgres_is_read_by_duckdb"
        ),
        pytest.param(MySQLSource(CONN), DuckDBEngine, id="mysql_is_read_by_duckdb"),
        pytest.param(MSSQLSource(CONN), DuckDBEngine, id="mssql_is_read_by_duckdb"),
        pytest.param(FilesSource(), DuckDBEngine, id="files_are_read_by_duckdb"),
        pytest.param(ArcGISSource(), DuckDBEngine, id="arcgis_is_read_by_duckdb"),
        pytest.param(GSheetsSource(), DuckDBEngine, id="gsheets_is_read_by_duckdb"),
        pytest.param(OracleSource(CONN), OracleEngine, id="oracle_has_its_engine"),
        pytest.param(CouchDBSource(CONN), CouchDBEngine, id="couchdb_has_its_engine"),
    ],
)
def test_a_source_gets_the_engine_that_serves_it(source, engine):
    """Return an unopened engine of the class that serves the source."""
    built = Engine.from_source(source)

    assert type(built) is engine


def test_a_source_that_no_engine_serves_is_refused():
    """Raise `TypeError` that names the source."""

    class Stranger:
        origin: None = None

    with pytest.raises(TypeError, match="Stranger"):
        Engine.from_source(Stranger())


@pytest.mark.parametrize("engine", load_engines())
def test_every_engine_implements_the_whole_base(engine):
    """Leave no abstract method, and say which sources it serves."""
    assert not engine.__abstractmethods__
    assert isinstance(engine.source_type, type)


def test_an_engine_reads_one_chunk_at_a_time_unless_it_says_otherwise():
    """Default `concurrency` to 1, and follow the source for CouchDB."""
    assert DuckDBEngine().concurrency == 1
    assert OracleEngine(OracleSource(CONN)).concurrency == 1
    assert CouchDBEngine(CouchDBSource(CONN, concurrency=3)).concurrency == 3


async def test_couchdb_refuses_sql():
    """Raise `UnsupportedRequestError`, naming the engine, for `execute` and `query`."""
    couchdb = CouchDBEngine(CouchDBSource(CONN))

    with pytest.raises(UnsupportedRequestError, match="CouchDBEngine"):
        await couchdb.execute("SELECT 1")

    with pytest.raises(UnsupportedRequestError, match="CouchDBEngine"):
        couchdb.query("SELECT 1")


def test_oracle_reads_chunks_and_no_whole_origin():
    """Raise `UnsupportedRequestError` for `read`, which only a chunk replaces."""
    with pytest.raises(UnsupportedRequestError, match="OracleEngine"):
        OracleEngine(OracleSource(CONN)).read()


def test_a_plain_duckdb_engine_has_no_origin_to_read():
    """Raise `ValueError` when a plain engine is asked for the origin of a source."""
    with pytest.raises(ValueError, match="no source"):
        DuckDBEngine().origin()


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(CouchDBEngine(CouchDBSource(CONN)), id="couchdb"),
        pytest.param(OracleEngine(OracleSource(CONN)), id="oracle"),
        pytest.param(MongoDBEngine(MongoDBSource(CONN, SCHEMA)), id="mongodb"),
    ],
)
async def test_an_engine_without_an_origin_refuses_to_describe(engine):
    """Raise `ValueError` for a source that names nothing to read."""
    with pytest.raises(ValueError, match="no origin"):
        await engine.describe()


def test_an_engine_whose_extra_is_missing_does_not_stop_the_others(
    monkeypatch, fail_to_import_engine
):
    """Load the other engines when the extra of MongoDB is not installed."""
    fail_to_import_engine(monkeypatch, "mongodb", "pymongoarrow")

    assert {DuckDBEngine, OracleEngine, CouchDBEngine} <= set(load_engines())


def test_an_import_error_of_another_module_is_not_forgiven(
    monkeypatch, fail_to_import_engine
):
    """Raise a missing module that does not belong to the extra of the engine."""
    fail_to_import_engine(monkeypatch, "mongodb", "not_the_extra")

    with pytest.raises(ModuleNotFoundError, match="not_the_extra"):
        load_engines()


def test_the_four_engines_implement_the_base_directly():
    """List the four engines, each one a direct subclass of `Engine`."""
    engines = load_engines()

    assert set(engines) >= {DuckDBEngine, OracleEngine, CouchDBEngine, MongoDBEngine}
    assert all(Engine in engine.__bases__ for engine in engines)


def test_oracle_runs_sql_where_the_other_engines_refuse_it():
    """Keep the `execute` and `query` of Oracle, and the refusal of the others."""
    assert OracleEngine.execute is not Engine.execute
    assert OracleEngine.query is not Engine.query
    assert CouchDBEngine.execute is Engine.execute
    assert MongoDBEngine.query is Engine.query


@pytest.mark.parametrize(
    ("kind", "width", "fits"),
    [
        pytest.param("BIGINT", 100, True, id="bigint_is_a_number_of_ids"),
        pytest.param("INTEGER", 100, True, id="integer_is_a_number_of_ids"),
        pytest.param("UBIGINT", 100, True, id="an_unsigned_integer_is_a_number_of_ids"),
        pytest.param("HUGEINT", 100, True, id="hugeint_is_a_number_of_ids"),
        pytest.param(
            "DECIMAL(19,0)", 100, True, id="a_decimal_with_no_scale_is_a_number_of_ids"
        ),
        pytest.param(
            "NUMERIC(10, 0)", 100, True, id="a_numeric_with_no_scale_is_a_number_of_ids"
        ),
        pytest.param("DECIMAL(10,2)", 100, False, id="a_decimal_with_a_scale_is_not"),
        pytest.param("DOUBLE", 100, False, id="a_double_is_not"),
        pytest.param("VARCHAR", 100, False, id="text_is_not_a_number_of_ids"),
        pytest.param("DATE", 100, False, id="a_date_is_not_a_number_of_ids"),
        pytest.param("INTERVAL", 100, False, id="an_interval_is_not_an_integer"),
        pytest.param("DATE", "month", True, id="a_date_fits_a_date_width"),
        pytest.param("TIMESTAMP", "day", True, id="a_timestamp_fits_a_date_width"),
        pytest.param(
            "TIMESTAMP WITH TIME ZONE", "year", True, id="a_zoned_timestamp_fits"
        ),
        pytest.param("TIMESTAMP_NS", "month", True, id="a_nanosecond_timestamp_fits"),
        pytest.param("INTEGER", "month", False, id="an_integer_is_not_a_date"),
        pytest.param("VARCHAR", "month", False, id="text_is_not_a_date"),
    ],
)
def test_the_chunk_column_must_fit_the_width_of_the_chunks(kind, width, fits):
    """Accept the types that a width can cut, and refuse the others."""
    chunks = Chunks(column="c", width=width, begin=0)
    engine = DuckDBEngine()

    if fits:
        engine.check_chunks(chunks, {"c": kind})
    else:
        with pytest.raises(ValueError, match="need"):
            engine.check_chunks(chunks, {"c": kind})


def test_regression_an_oracle_integer_is_accepted_for_a_number_of_ids():
    """Regression: an Oracle `NUMBER(19)` is `DECIMAL(19,0)`, and widths failed."""
    chunks = Chunks(column="ID", width=100, begin=0)

    OracleEngine(OracleSource(CONN)).check_chunks(chunks, {"ID": "DECIMAL(19,0)"})
