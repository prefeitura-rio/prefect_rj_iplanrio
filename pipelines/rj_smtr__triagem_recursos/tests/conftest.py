from collections.abc import Generator
from json import dumps
from pathlib import Path
from sys import path as sys_path
from typing import final

import pymupdf as fitz
import pytest
from httpx import Response

from pipelines.rj_smtr__triagem_recursos.helpers.triage import limpar_cache_cliente_llm

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys_path:
    sys_path.insert(0, str(REPO_ROOT))


@pytest.fixture(autouse=True)
def reset_llm_cache() -> Generator[None]:
    """Limpa o cache global de clientes LLM antes e depois de cada teste."""
    limpar_cache_cliente_llm()
    yield
    limpar_cache_cliente_llm()


def build_pdf_bytes(num_pages: int = 1, text_prefix: str = "Peticao de recurso") -> bytes:
    """Gera um PDF em memória RAM com a quantidade especificada de páginas."""
    doc = fitz.open()
    for page_idx in range(num_pages):
        page = doc.new_page()
        page.insert_text((72, 72), f"{text_prefix} pagina {page_idx + 1}")
    payload = doc.tobytes()
    doc.close()
    return payload


@final
class _DummyBooster:
    """Booster mínimo compatível com a interface preditora do LightGBM."""

    def __init__(self, prob: float) -> None:
        self.prob = prob

    def predict(self, _features: object) -> list[float]:
        """Retorna probabilidade fixa para simular predição do Booster."""
        return [self.prob]


@final
class DummyCalibrator:
    """Calibrador determinístico mínimo para testes unitários sem modelo em disco."""

    def __init__(self, prob: float = 0.96) -> None:
        """Inicializa calibrador simulado com probabilidade fixa."""
        self.prob = prob
        self.estatisticas_classes = None
        self.feature_names = ["fuzz_ratio"]
        self.medianas_imputacao = {"fuzz_ratio": 0.0}
        self.medias_scaler = {"fuzz_ratio": 0.0}
        self.desvios_scaler = {"fuzz_ratio": 1.0}
        self.booster = _DummyBooster(prob)


@final
class DummyConformal:
    """Estrutura conformal mínima compatível com a etapa de triagem."""

    def __init__(self, alpha: float = 0.05) -> None:
        """Inicializa predição conformal com nível de significância alpha."""
        self.alpha = alpha


def json_response(payload: dict[str, object], status_code: int = 200) -> Response:
    """Constrói uma resposta httpx JSON sintética com headers padrão."""
    return Response(
        status_code=status_code,
        content=dumps(payload).encode("utf-8"),
        headers={"content-type": "application/json"},
    )
