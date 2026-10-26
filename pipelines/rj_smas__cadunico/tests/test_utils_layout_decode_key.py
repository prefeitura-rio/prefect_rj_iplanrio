import pytest

from pipelines.rj_smas__cadunico.utils_layout import decode_key


def test_decode_key():
    cases = [("1", 1, "1"), ("1", 2, "01"), ("01", 1, "1"), ("01", 2, "01"), ("S", 1, "S"), ("01", 100, "01")]
    assert [decode_key(k, w) for k, w, _ in cases] == [out for _, _, out in cases]
    with pytest.raises(ValueError, match="nao cabe"):
        decode_key("101", 2)
