import pytest
from pydantic import JsonValue

from pipelines.rj_crm__agentforce_classificacao_llm.helpers.names import resolve_names
from pipelines.rj_crm__agentforce_classificacao_llm.helpers.sandbox import check_safe, compile_rules
from pipelines.rj_crm__agentforce_classificacao_llm.helpers.taxonomy import apply_rules, compile_catalog
from pipelines.rj_crm__agentforce_classificacao_llm.types import Row, Rule

PODA = "def poda(texto):\n    return 'poda' in texto or 'arvore' in texto\n"
ENTULHO = "import re\ndef entulho(texto):\n    return bool(re.search(r'entulho', texto))\n"


@pytest.mark.parametrize(
    ("code", "reason"),
    [
        pytest.param(PODA, None, id="a_plain_function"),
        pytest.param(ENTULHO, None, id="an_import_of_re"),
        pytest.param("import os\ndef f(t):\n    return True\n", "top level", id="an_unsafe_import"),
        pytest.param(
            "def f(t):\n    import subprocess\n    return True\n", "import not allowed", id="an_unsafe_import_inside"
        ),
        pytest.param("def f(t):\n    return eval('1')\n", "'eval'", id="a_forbidden_name"),
        pytest.param("def f(t):\n    return t.__class__\n", "dunder", id="a_dunder_attribute"),
        pytest.param("x = 1\ndef f(t):\n    return True\n", "top level", id="a_statement_at_the_top_level"),
        pytest.param("x = 1\n", "no function", id="no_function"),
        pytest.param("def f(:\n", "syntax error", id="a_syntax_error"),
    ],
)
def test_the_sandbox_refuses_code_that_is_not_safe(code: str, reason: str | None) -> None:
    """Name the first reason, or give none for safe code."""
    found = check_safe(code)

    assert (found is None) if reason is None else (reason in str(found))


def test_a_compiled_rule_runs_with_re_and_without_the_rest() -> None:
    """Compile each function of a block and call it."""
    rules = compile_rules(PODA + ENTULHO.replace("import re\n", ""))

    assert set(rules) == {"poda", "entulho"}
    assert rules["poda"]("quero poda") is True
    assert rules["entulho"]("entulho na rua") is True
    assert rules["poda"]("outra coisa") is False


def test_a_function_that_uses_a_name_from_outside_is_skipped() -> None:
    """Skip it and keep the good one of the same block."""
    rules = compile_rules("def bad(t):\n    return helper(t)\ndef good(t):\n    return len(t) > 1\n")

    assert set(rules) == {"good"}


def test_a_block_that_is_not_safe_raises() -> None:
    """Raise `ValueError` with the reason."""
    with pytest.raises(ValueError, match="refused for safety"):
        compile_rules("import os\ndef f(t):\n    return True\n")


def rule(name: str, code: str, secretariat: str = "Comlurb", parent: str | None = None) -> Rule:
    """Build a rule of the catalog."""
    return Rule(secretariat, parent, name, name.upper(), code)


def row(
    summary: str | None,
    secretariat: str | None = "Comlurb",
    *,
    relevant: bool | None = True,
    themes: list[JsonValue] | None = None,
) -> Row:
    """Build a row with a summary."""
    return {
        "id_sessao": "s",
        "resumo": summary,
        "secretaria_principal": secretariat,
        "conteudo_relevante": relevant,
        "tema_nome": themes if themes is not None else [],
    }


def test_the_rules_of_a_secretariat_fill_the_theme_of_its_relevant_sessions() -> None:
    """Match over the normalized summary, so a capital and an accent do not matter."""
    index = compile_catalog([rule("poda", PODA), rule("outro", "def outro(t):\n    return 'lixo' in t\n", "SMS")])
    rows = [
        row("Quero a PODA de uma Árvore"),
        row("lixo na rua", "SMS"),
        row("lixo na rua"),
        row("poda", relevant=False),
        row(None),
    ]

    result = apply_rules(rows, index, "tema_nome")

    assert [item["tema_nome"] for item in result] == [["PODA"], ["OUTRO"], [], [], []]


def test_a_session_without_a_secretariat_uses_the_rules_of_none_identified() -> None:
    """Fall back to `Sem secretaria identificada`."""
    index = compile_catalog(
        [rule("generica", "def generica(t):\n    return 'ajuda' in t\n", "Sem secretaria identificada")]
    )

    assert apply_rules([row("preciso de ajuda", None)], index, "tema_nome")[0]["tema_nome"] == ["GENERICA"]


def test_a_cause_runs_only_inside_the_theme_that_it_belongs_to() -> None:
    """Skip a rule whose parent theme the session does not have."""
    index = compile_catalog([rule("causa", "def causa(t):\n    return 'seca' in t\n", parent="PODA")])
    rows = [row("arvore seca", themes=["PODA"]), row("arvore seca", themes=["OUTRO"]), row("arvore seca")]

    result = apply_rules(rows, index, "causa_nome")

    assert [item["causa_nome"] for item in result] == [["CAUSA"], [], []]


def test_a_rule_that_fails_on_a_session_does_not_stop_the_others() -> None:
    """Skip the failing rule and keep the matches of the rest."""
    index = compile_catalog([rule("quebra", "def quebra(t):\n    return 1 / 0\n"), rule("poda", PODA)])

    assert apply_rules([row("poda")], index, "tema_nome")[0]["tema_nome"] == ["PODA"]


def test_a_rule_that_the_sandbox_refuses_is_skipped_and_an_empty_catalog_changes_nothing() -> None:
    """Leave the rows as they are when no rule compiles."""
    index = compile_catalog([rule("mau", "import os\ndef mau(t):\n    return True\n")])

    assert index == {}
    assert apply_rules([row("poda")], index, "tema_nome") == [row("poda")]


def test_the_names_of_the_tables_use_the_defaults_and_the_overrides() -> None:
    """Build the full name of each table from the project and the dataset."""
    names = resolve_names("p", {"dataset_id": "d", "table_id": "t"})

    assert names.destination == "p.d.t"
    assert names.tmp == "p.d.ai_agent_session_classificacao_tmp"
    assert names.rules == "p.d.taxonomia_regras"
    assert names.chatbot_v2 == "p.rmi_conversas.v2_chatbot_conversas"
    assert resolve_names("p", {"source_table": "x.y.z"}).source == "x.y.z"
