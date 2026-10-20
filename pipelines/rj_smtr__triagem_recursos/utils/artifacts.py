"""
Módulo de carregamento e caching dos artefatos estáticos de calibração.
Garante o uso da baseline congelada (LightGBM, Conformal CCCP e Centróides).
"""

from dataclasses import dataclass
import io
import os
from pathlib import Path
from typing import Any, Dict, Optional
import joblib

from ..constants import (
    GCS_ARTIFACTS_PREFIX,
    GCS_BUCKET_NAME,
    LOCAL_ARTIFACTS_DIR,
    STORAGE_BACKEND,
)
from .conformal import ArtefatosConforme
from .meta_calibrator import ModeloCalibrador
from .storage import _obter_cliente_gcs


@dataclass
class BaselineReferencia:
    """Container dos modelos e quantis estatísticos congelados da v3."""
    calibrador: ModeloCalibrador
    conformal: ArtefatosConforme
    prompt_sistema: str


_BASELINE_CACHE: Optional[BaselineReferencia] = None


def carregar_prompt_sistema(caminho_prompt: Optional[Path] = None) -> str:
    """Carrega o texto das instruções operacionais da v3."""
    if caminho_prompt and caminho_prompt.exists():
        with open(caminho_prompt, "r", encoding="utf-8") as f:
            return f.read()

    padrao = Path(__file__).resolve().parent.parent / "prompts" / "v3_baseline.txt"
    with open(padrao, "r", encoding="utf-8") as f:
        return f.read()


def carregar_baseline_referencia(
    forcar_recarga: bool = False,
    usar_local: Optional[bool] = None
) -> BaselineReferencia:
    """
    Carrega o Meta-Calibrador e os quantis conformais em memória uma única vez.
    """
    global _BASELINE_CACHE
    if _BASELINE_CACHE is not None and not forcar_recarga:
        return _BASELINE_CACHE

    is_local = (STORAGE_BACKEND == "local") if usar_local is None else usar_local

    if is_local:
        arquivo_calibrador = sorted(
            LOCAL_ARTIFACTS_DIR.glob("meta_calibrator_*.joblib"),
            key=lambda x: x.stat().st_mtime,
            reverse=True,
        )[0]
        arquivo_conformal = sorted(
            LOCAL_ARTIFACTS_DIR.glob("conformal_artifacts_*.joblib"),
            key=lambda x: x.stat().st_mtime,
            reverse=True,
        )[0]

        calibrador = joblib.load(arquivo_calibrador)
        conformal = joblib.load(arquivo_conformal)
    else:
        # Download direto dos bytes a partir do Google Cloud Storage
        client = _obter_cliente_gcs()
        bucket = client.bucket(GCS_BUCKET_NAME)

        blob_calibrador = bucket.blob(f"{GCS_ARTIFACTS_PREFIX}meta_calibrator_vertex_gemini_3.7_flash.joblib")
        blob_conformal = bucket.blob(f"{GCS_ARTIFACTS_PREFIX}conformal_artifacts_vertex_gemini_3.7_flash.joblib")

        buffer_cal = io.BytesIO(blob_calibrador.download_as_bytes())
        buffer_conf = io.BytesIO(blob_conformal.download_as_bytes())

        calibrador = joblib.load(buffer_cal)
        conformal = joblib.load(buffer_conf)

    prompt_texto = carregar_prompt_sistema()

    _BASELINE_CACHE = BaselineReferencia(
        calibrador=calibrador,
        conformal=conformal,
        prompt_sistema=prompt_texto,
    )
    return _BASELINE_CACHE