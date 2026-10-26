"""The sandbox that runs the rules of the catalog.

A rule is Python code that comes from a table, written by a model and checked by a
person. The sandbox reads its syntax tree before it runs it: it blocks imports other
than `re` and `unicodedata`, the dangerous names (`eval`, `exec`, `open`, `os`,
`subprocess` and others) and the dunder attributes, and it refuses a function that uses
a name that is not its own, a builtin of the safe list, `re` or `unicodedata`.
"""

import re
import unicodedata
from ast import (
    AsyncFunctionDef,
    Attribute,
    FunctionDef,
    Import,
    ImportFrom,
    Lambda,
    Load,
    Module,
    Name,
    NodeTransformer,
    Store,
    fix_missing_locations,
    parse,
    unparse,
    walk,
)
from collections.abc import Callable
from typing import cast, override

from toolkit.telemetry import logger

from pipelines.rj_crm__agentforce_classificacao_llm.constants import FORBIDDEN_NAMES, SAFE_IMPORTS

SAFE_BUILTINS: dict[str, object] = {
    "len": len,
    "any": any,
    "all": all,
    "str": str,
    "bool": bool,
    "int": int,
    "float": float,
    "min": min,
    "max": max,
    "sum": sum,
    "sorted": sorted,
    "list": list,
    "set": set,
    "tuple": tuple,
    "dict": dict,
    "range": range,
    "enumerate": enumerate,
    "zip": zip,
    "isinstance": isinstance,
    "True": True,
    "False": False,
    "None": None,
}
"""The builtins that a rule can use."""


class RemoveImports(NodeTransformer):
    """Take the imports out of a tree, as the sandbox gives the safe modules."""

    @override
    def visit_Import(self, node: Import) -> None:
        """Drop an import."""
        del node

    @override
    def visit_ImportFrom(self, node: ImportFrom) -> None:
        """Drop an import from a module."""
        del node


def is_safe_import(node: Import | ImportFrom) -> bool:
    """Tell whether an import is of `re` or `unicodedata` alone."""
    if isinstance(node, Import):
        return all(alias.name in SAFE_IMPORTS for alias in node.names)

    return node.module in SAFE_IMPORTS


def check_top_level(tree: Module) -> str | None:
    """Return why the top level of the code is unsafe, or nothing.

    It must hold functions and safe imports alone.
    """
    if not any(isinstance(node, FunctionDef) for node in tree.body):
        return "the block has no function"

    for node in tree.body:
        if not isinstance(node, FunctionDef) and not (isinstance(node, Import | ImportFrom) and is_safe_import(node)):
            return "only functions and imports of re and unicodedata are allowed at the top level"

    return None


def check_nodes(tree: Module) -> str | None:
    """Return why a node is unsafe: an unsafe import, a dunder or a forbidden name."""
    for node in walk(tree):
        if isinstance(node, Import | ImportFrom) and not is_safe_import(node):
            return "import not allowed"

        if isinstance(node, Attribute) and node.attr.startswith("__"):
            return "access to a dunder attribute is not allowed"

        if isinstance(node, Name) and node.id in FORBIDDEN_NAMES:
            return f"the name {node.id!r} is not allowed"

    return None


def check_safe(code: str) -> str | None:
    """Return why the code is not safe to run, or nothing if it is."""
    try:
        tree = parse(code)
    except SyntaxError as error:
        return f"the code has a syntax error: {error}"

    return check_top_level(tree) or check_nodes(tree)


def bound_names(function: FunctionDef) -> set[str]:
    """Return the names that a function binds itself.

    They are its parameters, variables and inner functions.
    """
    arguments = function.args
    names = {argument.arg for argument in [*arguments.args, *arguments.posonlyargs, *arguments.kwonlyargs]}
    names |= {argument.arg for argument in (arguments.vararg, arguments.kwarg) if argument}

    for node in walk(function):
        if node is function:
            continue

        if isinstance(node, Name) and isinstance(node.ctx, Store):
            names.add(node.id)
        elif isinstance(node, FunctionDef | AsyncFunctionDef):
            names.add(node.name)
            names |= {argument.arg for argument in node.args.args}
        elif isinstance(node, Lambda):
            names |= {argument.arg for argument in node.args.args}

    return names


def outside_references(function: FunctionDef) -> set[str]:
    """Return the names that a function reads from outside its own scope.

    A safe builtin, `re` and `unicodedata` do not count.
    """
    allowed = set(SAFE_BUILTINS) | {"re", "unicodedata", function.name} | bound_names(function)

    return {
        node.id
        for node in walk(function)
        if isinstance(node, Name) and isinstance(node.ctx, Load) and node.id not in allowed
    }


def compile_rules(code: str) -> dict[str, Callable[[str], bool]]:
    """Compile the functions of a block of rules, and return them by name.

    A function that uses a name from outside is skipped and logged. Raise `ValueError`
    if the block is not safe.
    """
    reason = check_safe(code)

    if reason:
        raise ValueError(f"The block is refused for safety: {reason}")

    tree = cast("Module", RemoveImports().visit(parse(code)))
    fix_missing_locations(tree)
    compiled: dict[str, Callable[[str], bool]] = {}

    for node in tree.body:
        if not isinstance(node, FunctionDef):
            continue

        outside = outside_references(node)

        if outside:
            logger.warning("A rule is refused for using %s: %s", sorted(outside), node.name)
            continue

        namespace: dict[str, object] = {"__builtins__": SAFE_BUILTINS, "re": re, "unicodedata": unicodedata}
        exec(unparse(Module(body=[node], type_ignores=[])), namespace)
        compiled[node.name] = namespace[node.name]  # pyright: ignore[reportArgumentType]  # the namespace holds the compiled functions

    return compiled
