"""The DuckDB functions that check and clean the documents and phones of Brazil.

Each one is a Polars expression of one text column. A NULL gives NULL, and a text that
is not valid gives `false` (`is_*`) or NULL (`normalize_*`).
"""

from collections.abc import Callable, Sequence

from duckdb.sqltypes import BOOLEAN, VARCHAR
from polars import Expr, Int64, element, lit, when

from ..constants import AREA_CODES
from .decorators import arrow


def build_cpf_weights(count: int) -> Sequence[int]:
    """Return the CPF weights of the first `count` digits, `count + 1` down to 2."""
    return range(count + 1, 1, -1)


def build_cnpj_weights(count: int) -> Sequence[int]:
    """Return the weights of the first `count` characters of a CNPJ, from 2 to 9."""
    return [position % 8 + 2 for position in reversed(range(count))]


def check_digit(values: Sequence[Expr], weights: Sequence[int]) -> Expr:
    """Return the mod 11 check digit of the weighted values, or NULL if one is NULL."""
    total = sum(
        (value * weight for value, weight in zip(values, weights, strict=True)),
        lit(0),
    )
    remainder = total % 11
    return when(remainder < 2).then(0).otherwise(11 - remainder)


def has_check_digits(
    values: Expr, size: int, weights: Callable[[int], Sequence[int]]
) -> Expr:
    """Tell if `size` values end in the two check digits of the others, and differ."""
    value = [values.list.get(position, null_on_oob=True) for position in range(size)]
    first = check_digit(value[: size - 2], weights(size - 2))
    second = check_digit(value[: size - 1], weights(size - 1))
    return (
        (values.list.len() == size)
        & (first == value[size - 2])
        & (second == value[size - 1])
        & (values.list.n_unique() > 1)
    )


def clean_cpf_text(column: Expr) -> Expr:
    """Keep the digits of a text."""
    return column.str.replace_all(r"\D", "")


def check_cpf(text: Expr) -> Expr:
    """Tell if a text of digits is a CPF."""
    digits = text.str.extract_all(r"\d").list.eval(element().cast(Int64))
    return has_check_digits(digits, 11, build_cpf_weights)


def clean_cnpj_text(column: Expr) -> Expr:
    """Keep the digits and the upper case letters of a text."""
    return column.str.to_uppercase().str.replace_all(r"[^0-9A-Z]", "")


def check_cnpj(text: Expr) -> Expr:
    """Tell if a text of digits and letters is a CNPJ, with the new letters too.

    The value of a character is its ASCII code minus 48, so `A` is 17.
    """
    code = element().str.to_integer(base=36)
    values = text.str.extract_all(r"[0-9A-Z]").list.eval(
        when(code >= 10).then(code + 7).otherwise(code)
    )
    return has_check_digits(values, 14, build_cnpj_weights)


@arrow(BOOLEAN)
def is_cpf(column: Expr) -> Expr:
    """Tell if a text is a CPF, with or without punctuation.

    ```sql
    SELECT is_cpf('529.982.247-25')  -- true
    ```
    """
    return check_cpf(clean_cpf_text(column))


@arrow(VARCHAR)
def normalize_cpf(column: Expr) -> Expr:
    """Return the 11 digits of a CPF, or NULL if it is not one.

    ```sql
    SELECT normalize_cpf('529.982.247-25')  -- '52998224725'
    ```
    """
    text = clean_cpf_text(column)
    return when(check_cpf(text)).then(text)


@arrow(BOOLEAN)
def is_cnpj(column: Expr) -> Expr:
    """Tell if a text is a CNPJ, with or without punctuation, and with letters too.

    ```sql
    SELECT is_cnpj('12.ABC.345/01DE-35')  -- true
    ```
    """
    return check_cnpj(clean_cnpj_text(column))


@arrow(VARCHAR)
def normalize_cnpj(column: Expr) -> Expr:
    """Return the 14 characters of a CNPJ in upper case, or NULL if it is not one.

    ```sql
    SELECT normalize_cnpj('12.abc.345/01de-35')  -- '12ABC34501DE35'
    ```
    """
    text = clean_cnpj_text(column)
    return when(check_cnpj(text)).then(text)


@arrow(BOOLEAN)
def is_phone(column: Expr) -> Expr:
    """Tell if a text is a landline or mobile phone of Brazil, with `55` or without.

    A number is a valid area code, then 8 digits that start with 2 to 5 (landline), or
    9 and 8 more digits (mobile).

    ```sql
    SELECT is_phone('+55 (21) 99876-5432')  -- true
    ```
    """
    digits = column.str.replace_all(r"\D", "")
    with_country = digits.str.starts_with("55") & digits.str.len_chars().is_in([12, 13])
    number = when(with_country).then(digits.str.slice(2)).otherwise(digits)
    shape = number.str.contains(r"^\d{2}([2-5]\d{7}|9\d{8})$")
    return number.str.slice(0, 2).is_in(sorted(AREA_CODES)) & shape
