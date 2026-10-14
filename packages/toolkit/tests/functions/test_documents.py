import pytest
from hypothesis import given
from hypothesis import strategies as st
from polars import Expr, lit, select

from toolkit.constants import AREA_CODES
from toolkit.functions import documents

CPF = "529.982.247-25"
CNPJ = "11.222.333/0001-81"
CNPJ_WITH_LETTERS = "12.ABC.345/01DE-35"


def cpf_with_digits(base: list[int]) -> str:
    """Return a CPF of 9 digits plus the check digits, by the textbook loop.

    ```python
    cpf_with_digits([5, 2, 9, 9, 8, 2, 2, 4, 7])  # "52998224725"
    ```
    """
    for _ in range(2):
        total = sum(d * (len(base) + 1 - i) for i, d in enumerate(base))
        base = [*base, 0 if total % 11 < 2 else 11 - total % 11]
    return "".join(map(str, base))


def cnpj_with_digits(base: list[int]) -> str:
    """Return a CNPJ of 12 digits plus the check digits, by the textbook loop.

    ```python
    cnpj_with_digits([1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 1])  # "12345678000195"
    ```
    """
    for _ in range(2):
        total = sum(d * (i % 8 + 2) for i, d in enumerate(reversed(base)))
        base = [*base, 0 if total % 11 < 2 else 11 - total % 11]
    return "".join(map(str, base))


def evaluate(expression: Expr) -> object:
    """Return the value of an expression on no frame."""
    return select(expression).item()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(f"'{CPF}'", True, id="a_cpf_with_punctuation"),
        pytest.param("'52998224725'", True, id="a_cpf_of_digits"),
        pytest.param("'529.982.247-24'", False, id="a_wrong_second_check_digit"),
        pytest.param("'529.982.247-35'", False, id="a_wrong_first_check_digit"),
        pytest.param(
            "'000.000.018-80'", False, id="a_wrong_first_digit_with_a_right_second"
        ),
        pytest.param("'111.111.111-11'", False, id="one_digit_repeated"),
        pytest.param("'5299822472'", False, id="too_short"),
        pytest.param("'529982247250'", False, id="too_long"),
        pytest.param("'abc'", False, id="no_digits"),
        pytest.param("''", False, id="empty"),
        pytest.param("NULL::VARCHAR", None, id="null_stays_null"),
    ],
)
async def test_is_cpf_checks_the_digits(scalar, value, expected):
    """Accept a CPF with the right check digits, and refuse the rest."""
    assert await scalar(f"is_cpf({value})") == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(f"'{CNPJ}'", True, id="a_cnpj_with_punctuation"),
        pytest.param("'11222333000181'", True, id="a_cnpj_of_digits"),
        pytest.param(f"'{CNPJ_WITH_LETTERS}'", True, id="a_cnpj_with_letters"),
        pytest.param("'12.abc.345/01de-35'", True, id="letters_in_lower_case"),
        pytest.param("'11.222.333/0001-82'", False, id="a_wrong_second_check_digit"),
        pytest.param("'11.222.333/0001-71'", False, id="a_wrong_first_check_digit"),
        pytest.param("'12.ABC.345/01DE-3A'", False, id="a_letter_as_check_digit"),
        pytest.param("'00.000.000/0000-00'", False, id="one_digit_repeated"),
        pytest.param("'1122233300018'", False, id="too_short"),
        pytest.param("''", False, id="empty"),
        pytest.param("NULL::VARCHAR", None, id="null_stays_null"),
    ],
)
async def test_is_cnpj_checks_the_characters(scalar, value, expected):
    """Accept a CNPJ with letters and right check digits, and refuse the rest."""
    assert await scalar(f"is_cnpj({value})") == expected


@pytest.mark.parametrize(
    ("function", "value", "expected"),
    [
        pytest.param("cpf", CPF, "52998224725", id="a_cpf_loses_its_punctuation"),
        pytest.param("cpf", "529.982.247-24", None, id="an_invalid_cpf_is_null"),
        pytest.param("cnpj", CNPJ, "11222333000181", id="a_cnpj_loses_its_punctuation"),
        pytest.param(
            "cnpj", "12.abc.345/01de-35", "12ABC34501DE35", id="letters_go_up"
        ),
        pytest.param("cnpj", "11.222.333/0001-82", None, id="an_invalid_cnpj_is_null"),
    ],
)
async def test_normalize_gives_the_characters_of_a_valid_document(
    scalar, function, value, expected
):
    """Return the characters of a valid CPF or CNPJ, and NULL for an invalid one."""
    assert await scalar(f"normalize_{function}('{value}')") == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("'(21) 99876-5432'", True, id="a_mobile"),
        pytest.param("'(21) 3456-7890'", True, id="a_landline"),
        pytest.param("'+55 (21) 99876-5432'", True, id="a_mobile_with_the_country"),
        pytest.param("'552134567890'", True, id="a_landline_with_the_country"),
        pytest.param("'(55) 99876-5432'", True, id="the_area_code_55_is_not_a_country"),
        pytest.param("'(10) 99876-5432'", False, id="an_area_code_that_does_not_exist"),
        pytest.param("'(21) 6456-7890'", False, id="a_landline_cannot_start_with_6"),
        pytest.param("'(21) 89876-5432'", False, id="a_mobile_must_start_with_9"),
        pytest.param("'(21) 9876-5432'", False, id="a_mobile_without_the_ninth_digit"),
        pytest.param("'+351 912 345 678'", False, id="another_country"),
        pytest.param("''", False, id="empty"),
        pytest.param("NULL::VARCHAR", None, id="null_stays_null"),
    ],
)
async def test_is_phone_accepts_a_phone_of_brazil(scalar, value, expected):
    """Accept a landline or mobile number, with the country code or not."""
    assert await scalar(f"is_phone({value})") == expected


@given(st.lists(st.integers(0, 9), min_size=9, max_size=9))
def test_a_cpf_with_the_right_check_digits_is_valid_and_a_changed_digit_is_not(base):
    """Accept every CPF that the textbook loop builds, and refuse a changed digit."""
    cpf = cpf_with_digits(base)
    changed = cpf[:10] + str((int(cpf[10]) + 1) % 10)

    assert evaluate(documents.is_cpf(lit(cpf))) == (len(set(cpf)) > 1)
    assert evaluate(documents.is_cpf(lit(changed))) is False


@given(st.lists(st.integers(0, 9), min_size=12, max_size=12))
def test_a_cnpj_with_the_right_check_digits_is_valid_and_a_changed_digit_is_not(base):
    """Accept every CNPJ that the textbook loop builds, and refuse a changed digit."""
    cnpj = cnpj_with_digits(base)
    changed = cnpj[:13] + str((int(cnpj[13]) + 1) % 10)

    assert evaluate(documents.is_cnpj(lit(cnpj))) == (len(set(cnpj)) > 1)
    assert evaluate(documents.is_cnpj(lit(changed))) is False


@given(
    st.sampled_from(sorted(AREA_CODES)),
    st.from_regex(r"[2-5][0-9]{7}|9[0-9]{8}", fullmatch=True),
)
def test_every_area_code_with_a_valid_number_is_a_phone(area_code, number):
    """Accept any assigned area code with a landline or a mobile number."""
    assert evaluate(documents.is_phone(lit(area_code + number))) is True
