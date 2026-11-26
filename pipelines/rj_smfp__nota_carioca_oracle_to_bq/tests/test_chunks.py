import re

import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.chunks import (
    LIKE_ESCAPE,
    TASK_PREFIX,
    chunk_task_name,
    drop_prefixed_tasks,
    drop_stale_task,
    prefix_like_pattern,
    validate_task_prefix,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import ExtractOptions


def like_to_regex(pattern: str) -> re.Pattern[str]:
    """Interpreta um LIKE com ESCAPE de barra invertida: ``%`` e ``_`` são curingas, a menos que escapados."""
    out: list[str] = []
    chars = iter(pattern)
    for char in chars:
        if char == LIKE_ESCAPE:
            out.append(re.escape(next(chars)))
        elif char == "%":
            out.append(".*")
        elif char == "_":
            out.append(".")
        else:
            out.append(re.escape(char))
    return re.compile("".join(out), re.DOTALL)


class FakeStore:
    """Tarefas em memória; ``list_by_prefix`` aplica o padrão real de LIKE."""

    def __init__(self, *names: str) -> None:
        self.names = list(names)
        self.dropped: list[str] = []

    def exists(self, task_name: str) -> bool:
        return task_name in self.names

    def list_by_prefix(self, prefix: str) -> list[str]:
        regex = like_to_regex(prefix_like_pattern(prefix))
        return [name for name in self.names if regex.fullmatch(name)]

    def drop(self, task_name: str) -> None:
        self.names.remove(task_name)
        self.dropped.append(task_name)


def test_prefix_pattern_escapes_the_underscore() -> None:
    assert prefix_like_pattern("O2BQ_") == "O2BQ\\_%"


def test_escaped_pattern_does_not_match_a_name_where_the_underscore_is_another_character() -> None:
    regex = like_to_regex(prefix_like_pattern())

    assert regex.fullmatch(chunk_task_name("PESSOAS", "run-1"))
    assert not regex.fullmatch("O2BQXPESSOAS_RUN")


def test_stale_task_with_the_same_name_is_dropped_before_creation() -> None:
    name = chunk_task_name("PESSOAS", "run-1")
    store = FakeStore(name)

    assert drop_stale_task(store, name) is True
    assert store.dropped == [name]


def test_nothing_is_dropped_when_the_task_does_not_exist() -> None:
    store = FakeStore(chunk_task_name("DPS", "run-1"))

    assert drop_stale_task(store, chunk_task_name("PESSOAS", "run-1")) is False
    assert store.dropped == []


def test_leftover_cleanup_drops_only_names_with_the_pipeline_prefix() -> None:
    mine = [chunk_task_name("PESSOAS", "run-1"), chunk_task_name("DPS", "run-1")]
    other = ["OTHER_TASK", f"{TASK_PREFIX[:-1]}XDPS_RUN"]
    store = FakeStore(*mine, *other)

    dropped = drop_prefixed_tasks(store)

    assert sorted(dropped) == sorted(mine)
    assert store.names == other


def test_the_cleanup_of_one_environment_keeps_the_tasks_of_another_on_the_same_user() -> None:
    # Given the homologation and the homteste, in the same database and with the same user, each with its prefix
    homologation = [chunk_task_name("DPS", "run-1"), chunk_task_name("NOTAS_NACIONAIS", "run-1")]
    homteste = [chunk_task_name("DPS", "run-2", "O2BQT_"), chunk_task_name("NOTAS_NACIONAIS", "run-2", "O2BQT_")]
    store = FakeStore(*homologation, *homteste)
    # When the homteste cleans its leftovers, Then the homologation tasks stay, and the other way around
    assert sorted(drop_prefixed_tasks(store, "O2BQT_")) == sorted(homteste)
    assert store.names == homologation
    assert sorted(drop_prefixed_tasks(store)) == sorted(homologation)


@pytest.mark.parametrize("prefix", ["O2BQ", "o2bq_", "O2BQ_T_", "_O2BQ_", "O2BQ%_", ""])
def test_a_prefix_that_could_overlap_another_is_refused(prefix: str) -> None:
    # A prefix is letters and digits ended by a single underscore, so that no valid prefix starts another one
    with pytest.raises(ValueError, match="chunk_task_prefix"):
        validate_task_prefix(prefix)


def test_the_extract_options_refuse_an_invalid_prefix() -> None:
    assert ExtractOptions().chunk_task_prefix == TASK_PREFIX
    assert ExtractOptions(chunk_task_prefix="O2BQT_").chunk_task_prefix == "O2BQT_"
    with pytest.raises(ValueError, match="chunk_task_prefix"):
        ExtractOptions(chunk_task_prefix="O2BQ_T_")
