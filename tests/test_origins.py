from types import SimpleNamespace

import pytest
from toolkit.executor.requests import Aggregate, Changes, Query
from toolkit.executor.requests import Table as TableOrigin

from utils import origins
from utils.origins import epsg_code, expand_database, expand_environment, read_request
from utils.types import Database


@pytest.mark.parametrize(
    ("db_type", "read", "batched", "expected"),
    [
        pytest.param("postgres", {"table": "public.orders"}, True, TableOrigin("public.orders"), id="a_table"),
        pytest.param("oracle", {"table": "APP.ORDERS"}, True, TableOrigin("APP.ORDERS"), id="a_table_of_oracle"),
        pytest.param("postgres", {"query": "SELECT 1"}, False, Query("SELECT 1", "postgres"), id="a_native_query"),
        pytest.param("bigquery", {"query": "SELECT 1"}, False, Query("SELECT 1", "bigquery"), id="a_bigquery_query"),
        pytest.param(
            "oracle",
            {"query": "SELECT 1 FROM dual"},
            True,
            Query("SELECT 1 FROM dual", "oracle"),
            id="a_query_of_oracle_is_cut",
        ),
        pytest.param("couchdb", {"table": "db"}, True, TableOrigin("db"), id="a_couchdb_database"),
        pytest.param("couchdb", {"query": "{}"}, False, Query("{}", "couchdb"), id="a_couchdb_selector"),
        pytest.param(
            "couchdb",
            {"table": "db", "query": "{}"},
            False,
            Query("{}", "couchdb", "db"),
            id="a_selector_of_a_database",
        ),
        pytest.param("couchdb", {"since": "0"}, False, Changes("0"), id="couchdb_changes"),
        pytest.param(
            "couchdb", {"table": "db", "since": "now"}, False, Changes("now", "db"), id="changes_of_a_database"
        ),
        pytest.param("mongodb", {"table": "pessoas"}, True, TableOrigin("pessoas"), id="a_mongodb_collection"),
        pytest.param(
            "mongodb",
            {"table": "p", "query": '{"a": 1}'},
            False,
            Query('{"a": 1}', "mongodb", "p"),
            id="a_mongodb_selector",
        ),
    ],
)
def test_a_read_names_the_origin_that_the_database_reads(db_type, read, batched, expected):
    """Give a table, a query or the changes of a database, as the type of database reads."""
    assert read_request(db_type, read, batched) == expected


@pytest.mark.parametrize(
    ("db_type", "read", "batched", "message"),
    [
        pytest.param("postgres", {}, False, "exactly one", id="neither"),
        pytest.param("postgres", {"table": "a.b", "query": "SELECT 1"}, False, "exactly one", id="both"),
        pytest.param("postgres", {"query": "SELECT 1"}, True, "one chunk", id="chunks_over_a_query"),
        pytest.param("couchdb", {}, False, "Give a table", id="a_database_with_nothing"),
        pytest.param("couchdb", {"query": "{}", "since": "0"}, False, "not both", id="query_and_since"),
        pytest.param("mongodb", {"table": "c", "since": "0"}, False, "only CouchDB", id="mongodb_has_no_changes"),
        pytest.param("postgres", {"table": "a.b", "pipeline": "stages"}, False, "only MongoDB", id="a_pipeline_on_sql"),
        pytest.param("mongodb", {"pipeline": "stages"}, False, "needs a table", id="a_pipeline_without_a_collection"),
        pytest.param(
            "mongodb",
            {"table": "c", "query": "{}", "pipeline": "stages"},
            False,
            "takes no query",
            id="pipeline_and_query",
        ),
    ],
)
def test_a_read_that_does_not_fit_is_refused(db_type, read, batched, message):
    """Raise before any connection, so a typo never reaches the database."""
    with pytest.raises(ValueError, match=message):
        read_request(db_type, read, batched)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("10.0.0.1", "10.0.0.1", id="a_value_without_a_placeholder"),
        pytest.param("${DB_HOST}", "db.test", id="the_whole_value"),
        pytest.param("mongodb://${DB_HOST}:${DB_PORT}", "mongodb://db.test:27017", id="two_placeholders"),
    ],
)
def test_a_placeholder_is_the_environment_variable_of_its_name(monkeypatch, value, expected):
    """Replace each `${NAME}` with the variable of the environment."""
    monkeypatch.setenv("DB_HOST", "db.test")
    monkeypatch.setenv("DB_PORT", "27017")

    assert expand_environment(value) == expected


@pytest.mark.parametrize(
    ("value", "variables", "message"),
    [
        pytest.param("${DB_HOST}", {}, "DB_HOST is not set", id="a_variable_that_is_missing"),
        pytest.param("${DB_HOST}", {"DB_HOST": ""}, "DB_HOST is not set", id="a_variable_that_is_empty"),
        pytest.param("${db_host}", {"DB_HOST": "x"}, "Invalid placeholder", id="a_name_in_lowercase"),
    ],
)
def test_a_placeholder_without_a_value_is_refused(monkeypatch, value, variables, message):
    """Raise at once, and name the variable, so a typo never reaches the connection."""
    monkeypatch.delenv("DB_HOST", raising=False)
    for name, text in variables.items():
        monkeypatch.setenv(name, text)

    with pytest.raises(ValueError, match=message):
        expand_environment(value)


def test_a_database_expands_its_host_and_its_name_only(monkeypatch):
    """Leave the type and the port as they are, and expand the host and the database."""
    monkeypatch.setenv("DB_HOST", "db.test")
    monkeypatch.setenv("DB_NAME", "taxi")
    database: Database = {"type": "mongodb", "host": "${DB_HOST}", "port": 27017, "database": "${DB_NAME}"}

    assert expand_database(database) == {"type": "mongodb", "host": "db.test", "port": 27017, "database": "taxi"}


@pytest.fixture
def pipeline_folder(tmp_path, monkeypatch):
    """Return the folder of the JSON files of the running pipeline `taxi_pipe`."""
    folder = tmp_path / "pipelines" / "taxi_pipe" / "pipelines"
    folder.mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(origins, "flow_run", SimpleNamespace(flow_name="taxi_pipe"))

    return folder


def test_a_mongodb_pipeline_is_read_by_name_from_the_folder_of_the_pipeline(pipeline_folder):
    """Read the stages of `<name>.json`, whatever its layout, as one compact text."""
    (pipeline_folder / "ranking-races.json").write_text(
        '[\n  {"$project": {"a": 1}},\n  {"$limit": 5}\n]\n', encoding="utf-8"
    )

    origin = read_request("mongodb", {"table": "p", "pipeline": "ranking-races"}, True)

    assert origin == Aggregate('[{"$project":{"a":1}},{"$limit":5}]', "p")


@pytest.mark.parametrize(
    "content",
    [
        pytest.param("[1]", id="a_stage_that_is_a_number"),
        pytest.param('{"$limit": 5}', id="a_stage_without_a_list"),
        pytest.param("[{", id="text_that_is_not_json"),
    ],
)
def test_a_pipeline_file_that_is_not_a_list_of_stages_is_refused(pipeline_folder, content):
    """Raise before any connection when the file is not a JSON list of objects."""
    (pipeline_folder / "stages.json").write_text(content, encoding="utf-8")

    with pytest.raises(ValueError, match="JSON list of stages"):
        read_request("mongodb", {"table": "c", "pipeline": "stages"}, False)


@pytest.mark.parametrize("name", ["../secret", "Ranking", "ranking_races", "a/b", "-a", ""])
def test_a_pipeline_name_is_words_with_dashes(name):
    """Refuse a name that is not lowercase words with dashes, so it never leaves the folder."""
    with pytest.raises(ValueError, match="words with dashes"):
        read_request("mongodb", {"table": "c", "pipeline": name}, False)


@pytest.mark.parametrize(
    ("crs", "expected"),
    [
        pytest.param("EPSG:31983", "31983", id="with_the_prefix"),
        pytest.param("epsg:3857", "3857", id="in_lower_case"),
        pytest.param("4326", "4326", id="the_number_only"),
    ],
)
def test_an_epsg_code_is_its_number(crs, expected):
    """Give the number that ArcGIS takes as `outSR`."""
    assert epsg_code(crs) == expected


@pytest.mark.parametrize("crs", ["WGS84", "EPSG:", "EPSG:31983; DROP", ""])
def test_a_text_that_is_not_an_epsg_code_is_refused(crs):
    """Raise before the number goes into a URL."""
    with pytest.raises(ValueError, match="EPSG"):
        epsg_code(crs)
