import re

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.chunks import (
    LIKE_ESCAPE,
    TASK_PREFIX,
    chunk_task_name,
    drop_prefixed_tasks,
    drop_stale_task,
    prefix_like_pattern,
)


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
