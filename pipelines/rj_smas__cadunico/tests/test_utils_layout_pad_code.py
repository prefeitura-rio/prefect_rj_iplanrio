import pytest

from pipelines.rj_smas__cadunico.utils_layout import pad_code


def test_pad_code():
    cases = [("1", 1, "1"), ("1", 2, "01"), ("01", 1, "1"), ("01", 2, "01"), ("S", 1, "S"), ("01", 100, "01")]
    assert [pad_code(k, w) for k, w, _ in cases] == [out for _, _, out in cases]
    with pytest.raises(ValueError, match="nao cabe"):
        pad_code("101", 2)
    with pytest.raises(ValueError, match="nao cabe"):
        pad_code("0", 0)
