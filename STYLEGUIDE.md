# Repository style guide

Use this guide when you add or change a pipeline. It is for developers who know Python but are new to this repository.

The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** show the strength of a rule. A rule with an `.ast-grep/rules/*.yaml` path has an executable ast-grep check in that file. `lint:all` runs every automated check. A rule with `Check: review` needs human judgment.

Run this command before you request review:

```sh
devenv --profile checks tasks run lint:all test:all
```

## 1. Start a pipeline

Use the template:

```sh
uvx cookiecutter templates --output-dir=pipelines
```

Choose `dump_database`, `ingest_api`, `dump_arcgis`, or `dump_gsheet`. Replace every example value. A ready pipeline has a clear package description and valid staging and production deployments.

There are two designs:

1. A standard pipeline imports a ready flow from `utils.flows`. It puts configuration in `prefect.yaml`.
2. A custom pipeline keeps orchestration in `flow.py`, Prefect tasks in `tasks.py`, and domain behavior in `helpers.py` or `helpers/`.

Search `src/utils/` and `packages/toolkit/README.md` before you write a helper.

## 2. Code layers

### layer-owner

- **Level:** MUST
- **Check:** review
- **Rule:** Put code in the lowest layer that owns its behavior.
- **Why:** One owner prevents duplicate implementations.

Compliant:

```text
packages/toolkit/   Generic primitive used by repositories
src/utils/          Shared behavior used by two pipelines in this repository
pipelines/rj_x__y/  Behavior used by one pipeline
```

Non-compliant:

```text
src/utils/taxirio.py  # TaxiRio-only behavior in the shared layer
```

### promote-on-second-use

- **Level:** MUST
- **Check:** review
- **Rule:** Move a helper to `src/utils/` only when a second pipeline uses it.
- **Why:** An unused abstraction makes code harder to change.

Compliant:

```python
# Keep this in one pipeline until another pipeline needs it.
def parse_vendor_date(value: str) -> date: ...
```

Non-compliant:

```python
# Do not add this to src/utils/ because it might be useful later.
def parse_possible_future_vendor_date(value: str) -> date: ...
```

## 3. Pipeline files

### required-pipeline-files

- **Level:** MUST
- **Check:** `lint:layout`
- **Rule:** Each enabled pipeline has `flow.py`, `prefect.yaml`, `pyproject.toml`, and `Dockerfile`.
- **Why:** The build and deploy tools depend on these files.

Compliant:

```text
pipelines/rj_sec__items/
├── Dockerfile
├── flow.py
├── prefect.yaml
└── pyproject.toml
```

Non-compliant:

```text
pipelines/rj_sec__items/
└── pipeline.py
```

### forbidden-pipeline-files

- **Level:** MUST NOT
- **Check:** `lint:layout`
- **Rule:** Do not add `__init__.py`, `utils.py`, `utils/`, `env.py`, `schedules.py`, `build_prefect_yaml.py`, `.python-version`, loose test scripts, or generated migration notes.
- **Why:** These files shadow shared modules or create a second source of truth.

Compliant:

```text
helpers/api.py
tests/test_api.py
queries/read_items.sql
```

Non-compliant:

```text
utils/api.py
schedules.py
test_api.py
MIGRATION_NOTES.md
```

### data-file-location

- **Level:** MUST
- **Check:** `lint:layout`
- **Rule:** Put SQL in `queries/`, test data in `tests/`, and Mongo aggregation JSON in `pipelines/`.
- **Why:** A reader can find data files without searching the package.

Compliant:

```text
queries/read_items.sql
tests/fixtures/response.json
pipelines/races.json
```

Non-compliant:

```text
helpers/read_items.sql
response.json
```

## 4. Module responsibilities

### one-flow

- **Level:** MUST
- **Check:** `lint:layout`
- **Rule:** `flow.py` defines or assigns exactly one flow whose name equals the pipeline directory.
- **Why:** Prefect and the deploy script use that name as the entry point.

Compliant:

```python
@flow(log_prints=True)
def rj_sec__items() -> None:
    """Load the items."""
    load_items_task()
```

Non-compliant:

```python
@flow
def load_items(): ...

@flow
def retry_items(): ...
```

### no-task-in-flow

- **Level:** MUST NOT
- **Check:** `.ast-grep/rules/no-task-in-flow.yaml`
- **Rule:** Do not define a task in `flow.py`.
- **Why:** A task belongs to the task boundary in `tasks.py`.

Compliant:

```python
from pipelines.rj_sec__items.tasks import load_items_task
```

Non-compliant:

```python
@task
def load_items_task() -> None: ...
```

### no-flow-in-tasks

- **Level:** MUST NOT
- **Check:** `.ast-grep/rules/no-flow-in-tasks.yaml`
- **Rule:** Do not define a flow in `tasks.py`.
- **Why:** A flow belongs to `flow.py`.

Compliant:

```python
@task
def load_items_task() -> None: ...
```

Non-compliant:

```python
@flow
def child_flow() -> None: ...
```

### orchestration-only-flow

- **Level:** MUST
- **Check:** `.ast-grep/rules/no-io-in-flow.yaml` and review
- **Rule:** A pipeline `flow.py` only calls tasks, handles their results, and defines their order.
- **Why:** Domain behavior must remain testable without Prefect.

Compliant:

```python
@flow(log_prints=True)
def rj_sec__items() -> None:
    """Load the items."""
    rows = read_items_task()
    write_items_task(rows)
```

Non-compliant:

```python
@flow(log_prints=True)
def rj_sec__items() -> None:
    """Load the items."""
    response = httpx2.get(URL)
    rows = [clean(row) for row in response.json()]
```

### thin-task

- **Level:** MUST
- **Check:** review
- **Rule:** A task does one orchestration operation and delegates domain behavior to a helper or toolkit function.
- **Why:** Retries and state belong to Prefect. Parsing and I/O behavior need direct unit tests.

Compliant:

```python
@task(retries=3)
def read_items_task(url: str) -> list[Item]:
    """Read the items."""
    return read_items(url)
```

Non-compliant:

```python
@task
def read_items_task(url: str) -> list[Item]:
    """Read the items."""
    # 80 lines of HTTP, parsing, validation, and storage behavior
```

### no-prefect-in-helpers

- **Level:** MUST NOT
- **Check:** `.ast-grep/rules/no-prefect-in-helpers.yaml`
- **Rule:** Do not import any Prefect module in `helpers.py` or `helpers/`.
- **Why:** Helpers must run in a unit test without a Prefect runtime.

Compliant:

```python
from toolkit.telemetry import logger
```

Non-compliant:

```python
from prefect.runtime import deployment
```

## 5. Python

### complete-types

- **Level:** MUST
- **Check:** basedpyright
- **Rule:** Add a type to every function parameter and return value.
- **Why:** Types find integration errors before a flow runs.

Compliant:

```python
def parse_count(value: str) -> int:
    """Parse the count."""
    return int(value)
```

Non-compliant:

```python
def parse_count(value):
    return int(value)
```

### modern-types

- **Level:** MUST
- **Check:** Ruff and `.ast-grep/rules/no-legacy-typing.yaml`
- **Rule:** Use `list[str]` and `str | None`, not `List[str]` and `Optional[str]`.
- **Why:** The repository targets Python 3.14.

Compliant: `dict[str, int] | None`

Non-compliant: `Optional[Dict[str, int]]`

### no-future-import

- **Level:** MUST NOT
- **Check:** `.ast-grep/rules/no-future-import.yaml`
- **Rule:** Do not use `__future__` imports in production code.
- **Why:** The repository targets Python 3.14 and does not need compatibility imports.

Compliant:

```python
from typing import cast
```

Non-compliant:

```python
from __future__ import annotations
```

### no-private-functions

- **Level:** MUST NOT
- **Check:** `.ast-grep/rules/no-private-functions.yaml`
- **Rule:** Do not prefix a production function with `_`.
- **Why:** The module boundary controls visibility.

Compliant: `def read_rows() -> list[Row]: ...`

Non-compliant: `def _read_rows() -> list[Row]: ...`

### explicit-imports

- **Level:** MUST
- **Check:** Ruff and `.ast-grep/rules/no-wildcard-import.yaml`
- **Rule:** Import the names you use. Do not use wildcard imports.
- **Why:** Explicit imports show dependencies and support static analysis.

Compliant:

```python
from asyncio import to_thread
import polars as pl
```

Non-compliant:

```python
from helpers import *
```

Module imports are permitted when the module is an API object, a test monkeypatch target, or a standard data alias (`pl`, `np`, `pa`). Prefer a name import otherwise.

### no-print

- **Level:** MUST NOT
- **Check:** Ruff `T20` and `.ast-grep/rules/no-print.yaml`
- **Rule:** Do not call `print()` in repository production code.
- **Why:** The repository logger sends messages to Prefect and telemetry.

Compliant:

```python
from toolkit.telemetry import logger

logger.info("Items read: rows=%d", len(rows))
```

Non-compliant:

```python
print(f"Read {len(rows)} rows")
```

### log-safely

- **Level:** MUST
- **Check:** `.ast-grep/rules/no-stdlib-logger.yaml` and review
- **Rule:** Use `toolkit.telemetry.logger`, or `get_run_logger()` inside a task when the message needs the task context.
- **Why:** Other loggers can lose INFO messages and telemetry labels.

Compliant: `logger.info("Load complete: rows=%d", count)`

Non-compliant: `logging.getLogger(__name__).info("done")`

Never log a credential, token, connection string, request body, row value, SQL text, or query string.

### docstrings

- **Level:** MUST
- **Check:** Ruff `D` and `W505`
- **Rule:** Give each production module, function, and class a short English Markdown docstring.
- **Why:** A reader needs the contract without reading the implementation.

Compliant:

```python
def read_items(path: Path) -> list[Item]:
    """Read and validate the items.

    Raise `ValueError` if an item has no id.
    """
```

Non-compliant:

```python
def read_items(path: Path) -> list[Item]:
    ":param path: input file"
```

### no-base-exception

- **Level:** MUST NOT
- **Check:** `.ast-grep/rules/no-base-exception.yaml`
- **Rule:** Do not raise `BaseException`.
- **Why:** `BaseException` also represents process controls such as `KeyboardInterrupt`.

Compliant: `raise ValueError("The id is empty")`

Non-compliant: `raise BaseException("The id is empty")`

Catching `BaseException` is permitted only to release a resource before the same exception is raised again.

### no-assert-in-production

- **Level:** MUST NOT
- **Check:** `.ast-grep/rules/no-assert-in-production.yaml`
- **Rule:** Do not use `assert` to validate production data.
- **Why:** Python can remove assertions and their errors lack domain context.

Compliant:

```python
if not rows:
    raise ValueError("The source returned no rows")
```

Non-compliant: `assert rows`

### pydantic-v2

- **Level:** MUST
- **Check:** `.ast-grep/rules/no-pydantic-v1-validator.yaml`
- **Rule:** Use `field_validator`, not the Pydantic v1 `validator` decorator.
- **Why:** The repository uses Pydantic v2.

Compliant: `@field_validator("name")`

Non-compliant: `@validator("name")`

### grouped-parameters

- **Level:** SHOULD
- **Check:** Ruff `PLR0913`, basedpyright, and review
- **Rule:** Group more than five related parameters in a dataclass or TypedDict.
- **Why:** A named group is easier to validate and extend.

Compliant:

```python
@dataclass(frozen=True)
class FetchConfig:
    url: str
    timeout: float
    page_size: int
```

Non-compliant: `def fetch(url, timeout, page_size, user, password, retries): ...`

Use a TypedDict for flow and task parameters because Prefect serializes them. Put a group that two modules use in `types.py`.

### constant-location

- **Level:** MUST
- **Check:** review
- **Rule:** Keep a module-only constant in that module. Move it to `constants.py` when a second module uses it.
- **Why:** This keeps one source of truth without creating a dumping ground.

Compliant:

```python
# helpers/api.py, used only here
TIMEOUT_SECONDS = 30.0
```

Compliant when shared:

```python
# constants.py
TIMEOUT_SECONDS = 30.0
```

Non-compliant: duplicate `TIMEOUT_SECONDS = 30.0` in two modules.

## 6. Data and SQL

### dataframe-tools

- **Level:** MUST
- **Check:** Ruff `TID251`
- **Rule:** Use Polars for dataframes and the toolkit Executor for DuckDB SQL.
- **Why:** One data stack gives consistent types and memory behavior.

Compliant:

```python
frame = pl.scan_parquet(path).filter(pl.col("active")).collect()
```

Non-compliant:

```python
import pandas as pd
import duckdb
```

### sql-files

- **Level:** MUST
- **Check:** SQLFluff and review
- **Rule:** Put executable SQL in one `queries/*.sql` Jinja template per operation.
- **Why:** SQL files are lintable and reviewable.

Compliant:

```sql
{# {"kind":"template","description":"Read active items.","inputs":{}} #}
SELECT id FROM src.items WHERE active
```

Non-compliant:

```python
query = f"SELECT * FROM `{project}.{dataset}`"
```

The `read.query` value of a standard `dump_database` schedule is the only pipeline YAML exception. It contains the native source query.

### validate-at-boundary

- **Level:** MUST
- **Check:** review and tests
- **Rule:** Validate an external dataframe with a Polars schema and validate configuration with Pydantic v2.
- **Why:** A failure should occur where bad data enters.

Compliant: `pl.DataFrame(rows, schema=ITEM_SCHEMA)`

Non-compliant: read untyped data and cast fields throughout the pipeline.

## 7. Settings and secrets

### settings-only

- **Level:** MUST
- **Check:** `.ast-grep/rules/no-environment-read.yaml` and `lint:env`
- **Rule:** Read environment values through a `ToolkitSettings` subclass in `settings.py`.
- **Why:** Settings give one validated boundary and clear missing-variable errors.

Compliant:

```python
class Settings(ToolkitSettings):
    API_TOKEN: SecretStr = required_secret()
```

Non-compliant: `token = environ["API_TOKEN"]`

Use `<SERVICE>_<ATTRIBUTE>` names in upper snake case. Do not put `PROD`, `STAGING`, or the pipeline name in a variable name. Use ADC for Google credentials.

## 8. Prefect deployments

### no-if-main-in-flow

- **Level:** MUST NOT
- **Check:** `.ast-grep/rules/no-if-main-in-flow.yaml`
- **Rule:** Do not run a flow from an `if __name__ == "__main__"` block.
- **Why:** Deployments own flow execution.

Compliant: `prefect flow-run execute`

Non-compliant:

```python
if __name__ == "__main__":
    rj_sec__items()
```

### schedules-in-prefect-yaml

- **Level:** MUST
- **Check:** `lint:layout` and `lint:prefect`
- **Rule:** Keep production schedules only in `prefect.yaml`. Staging has no schedules.
- **Why:** One source of truth prevents accidental duplicate runs.

Compliant:

```yaml
- name: rj-sec--items--prod
  schedules:
    - cron: "0 6 * * *"
      timezone: America/Sao_Paulo
      slug: daily
```

Non-compliant: `schedules.py`

### deployment-name

- **Level:** MUST
- **Check:** `lint:prefect`
- **Rule:** Name deployments `rj-<owner>--<pipeline>--staging` and `rj-<owner>--<pipeline>--prod`.
- **Why:** Tools identify the environment from the name.

Compliant: `rj-iplanrio--items--prod`

Non-compliant: `rj_iplanrio__items_prod`

### deployment-tags

- **Level:** MUST
- **Check:** `lint:prefect`
- **Rule:** Each deployment has `environment`, `severity`, and `code_owner` tags.
- **Why:** Alerts and searches use these tags.

Compliant:

```yaml
tags:
  - environment:prod
  - severity:high
  - code_owner:developer
```

Non-compliant:

```yaml
tags:
  - prod
```

Severity is `low`, `medium`, `high`, or `critical`. Use `unassigned` only until a real owner accepts the pipeline.

### work-pool

- **Level:** MUST
- **Check:** `lint:prefect` and review
- **Rule:** Use `default-pool` for GKE, `onprem-pool` for internal systems, and `taxirio-pool` only for TaxiRio.
- **Why:** The pool selects network access and credentials.

Compliant: an Oracle pipeline uses `onprem-pool` and `pipelines-prod`.

Non-compliant: an internal Oracle pipeline uses `default-pool`.

### schedule-timezone

- **Level:** MUST
- **Check:** `lint:prefect`
- **Rule:** Give every schedule a slug and `America/Sao_Paulo`, unless a documented UTC contract requires UTC.
- **Why:** Explicit timezones prevent daylight and deployment errors.

Compliant: `timezone: America/Sao_Paulo`

Non-compliant: a cron schedule with no timezone.

### data-dependency-first

- **Level:** SHOULD
- **Check:** review
- **Rule:** Pass a task result to its consumer instead of using `wait_for` when a data dependency exists.
- **Why:** Prefect can infer the order and preserve the value contract.

Compliant: `write_task(read_task())`

Non-compliant: `write_task(wait_for=[read_task()])` when `write_task` needs the rows.

## 9. Package metadata

### package-metadata

- **Level:** MUST
- **Check:** `lint:deps` and review
- **Rule:** Set pipeline version to `1.0.0`, write a useful English description, and list only imported packages.
- **Why:** Git identifies releases and the workspace owns shared versions.

Compliant:

```toml
version = "1.0.0"
description = "Loads Example API items for service monitoring."
dependencies = ["prefect_rj_iplanrio", "polars"]
```

Non-compliant:

```toml
version = "2.3"
description = "TODO"
dependencies = ["prefect==3.8.8"]
```

## 10. Tests

### test-domain-behavior

- **Level:** MUST
- **Check:** pytest and review
- **Rule:** Test each parser, branch, loop, security path, and money path at the lowest useful level.
- **Why:** Tests must fail when observable behavior breaks.

Compliant:

```python
@pytest.mark.parametrize(
    ("raw", "expected"),
    [pytest.param("1,5", 1.5, id="decimal-comma")],
)
def test_parse_value(raw: str, expected: float) -> None:
    """Parse a decimal comma."""
    assert parse_value(raw) == expected
```

Non-compliant: a test that only checks that no exception occurs.

Pipeline flows need no direct test when they only connect tasks. Shared flow builders in `src/utils/flows.py` need tests because they implement reusable behavior.

Tests use no production network, credentials, or Prefect runtime. Use table cases for multiple inputs. Give each regression test `Regression` in its docstring.

## 11. Repository hygiene

### no-generated-junk

- **Level:** MUST NOT
- **Check:** `lint:layout` and `.gitignore`
- **Rule:** Do not commit caches, local environments, debug exports, generated notes, or empty Python files.
- **Why:** Git stores source, not workstation state.

Compliant: put temporary evidence in `/mnt/external/pi`.

Non-compliant: commit `.venv/`, `__pycache__/`, `.DS_Store`, or `MIGRATION_NOTES.md`.

### commit-style

- **Level:** MUST
- **Check:** review
- **Rule:** Use one logical change per Conventional Commit: `type(scope): lowercase imperative summary`.
- **Why:** A focused history supports review and rollback.

Compliant: `fix(taxirio): preserve month chunk labels`

Non-compliant: `updates`

Valid types are `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `data`, and `chore`.

## 12. Pull request checklist

- [ ] The directory and required files follow section 3.
- [ ] `flow.py`, `tasks.py`, and helpers follow section 4.
- [ ] No secret or data value appears in a log or diff.
- [ ] SQL and settings use their required files.
- [ ] Staging has no schedule. Production schedules have slugs and timezones.
- [ ] New behavior has a focused test.
- [ ] `devenv --profile checks tasks run lint:all test:all` passes.
