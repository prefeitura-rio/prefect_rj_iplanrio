"""Carregamento, caching em memória e ciclo de vida dos artefatos de calibração da pipeline.

Descrição Técnica & Arquitetura:
Centraliza a inicialização dos artefatos essenciais para a operação da esteira estatística
e do modelo de linguagem. Implementa uma estratégia de carregamento resiliente com duas camadas:
1. Fonte Primária (Banco de Dados): busca a configuração ativa na tabela `configuracoes_calibracao`
   do PostgreSQL / Cloud SQL, reconstituindo o modelo LightGBM textual, as medianas de imputação,
   os desvios z-score e os quantis conformais CCCP.
2. Fallback de Contingência (Joblib Congelado): caso o banco esteja indisponível ou não possua
   configuração ativa, carrega a baseline serializada do disco (`.joblib`), garantindo continuidade
   operacional ininterrupta.

Fluxo de Dependências:
- Consumidores Diretos:
  * `tasks.py`: invocado por `task_carregar_artefatos_referencia` no início da execução da pipeline.
- Dependências Diretas:
  * `constants.py`: importa caminhos locais de prompts e artefatos, bem como a versão padrão de prompt.
  * `utils/db_repository.py`: invoca `buscar_configuracao_calibracao_ativa` para consultar o banco.
  * `utils/meta_calibrator.py`: utiliza `criar_calibrador_dos_parametros_banco` para compor o `ModeloCalibrador`.
  * `utils/conformal.py`: utiliza `criar_artefatos_conforme_do_banco` para compor `ArtefatosConforme`.
  * `prefect.logging`: logger com rastreabilidade OpenTelemetry.
"""

import os
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
from toolkit.telemetry import logger

from pipelines.rj_smtr__triagem_recursos.constants import (
    LOCAL_ARTIFACTS_DIR,
    PROMPTS_DIR,
    VERSAO_PROMPT_PADRAO,
    resolver_banco_url,
)
from pipelines.rj_smtr__triagem_recursos.helpers.conformal import (
    ArtefatosConforme,
    criar_artefatos_conforme_do_banco,
)
from pipelines.rj_smtr__triagem_recursos.helpers.meta_calibrator import (
    ModeloCalibrador,
    criar_calibrador_dos_parametros_banco,
)


def registrar_aliases_legados() -> None:
    """Registra aliases em sys.modules para desserialização transparente de joblib."""
    from pipelines.rj_smtr__triagem_recursos.helpers import (
        conformal,
        feature_extractor,
        meta_calibrator,
    )

    if "src" not in sys.modules:
        sys.modules["src"] = types.ModuleType("src")
    if "src.core" not in sys.modules:
        sys.modules["src.core"] = types.ModuleType("src.core")

    sys.modules["src.core.meta_calibrator"] = meta_calibrator
    sys.modules["src.core.feature_extractor"] = feature_extractor
    sys.modules["src.core.conformal"] = conformal


@dataclass
class BaselineReferencia:
    """Container dos modelos e quantis estatísticos congelados da v3."""

    calibrador: ModeloCalibrador
    conformal: ArtefatosConforme
    prompt_sistema: str


BASELINE_CACHE: BaselineReferencia | None = None


def carregar_prompt_sistema(
    caminho_prompt: Path | None = None,
    infisical_secret_path: str | None = None,
    is_local: bool = False,
) -> str:
    """Carrega o texto das instrucoes operacionais via Infisical/ambiente (ou disco em modo local)."""
    for var_nome in ("PROMPT_SISTEMA_V4", "PROMPT_SISTEMA", "SMTR_TRIAGEM_PROMPT"):
        valor_env = os.environ.get(var_nome)
        if valor_env and len(valor_env.strip()) > 50:
            logger.info("Prompt carregado com sucesso a partir da variavel de ambiente (%s).", var_nome)
            return valor_env.strip()

    if infisical_secret_path:
        chave_derivada = infisical_secret_path.strip("/").replace("-", "_").upper() + "_PROMPT"
        valor_derivado = os.environ.get(chave_derivada)
        if valor_derivado and len(valor_derivado.strip()) > 50:
            logger.info("Prompt carregado via chave derivada do Infisical (%s).", chave_derivada)
            return valor_derivado.strip()

    if not is_local:
        raise RuntimeError(
            "Variavel de ambiente PROMPT_SISTEMA_V4 nao localizada no Infisical para execucao em cloud/staging."
        )

    if caminho_prompt and caminho_prompt.exists():
        return caminho_prompt.read_text(encoding="utf-8")

    padrao = PROMPTS_DIR / f"{VERSAO_PROMPT_PADRAO}.txt"
    if padrao.exists():
        return padrao.read_text(encoding="utf-8")

    arquivos = sorted(PROMPTS_DIR.glob("*.txt"))
    if arquivos:
        return arquivos[0].read_text(encoding="utf-8")

    raise FileNotFoundError("Nenhum arquivo de prompt localizado em prompts/ ou no ambiente.")


def carregar_baseline_referencia(
    forcar_recarga: bool = False,
    diretorio_artefatos: Path | None = None,
) -> BaselineReferencia:
    """Carrega o Meta-Calibrador e os quantis conformais em memoria uma unica vez."""
    global BASELINE_CACHE
    if BASELINE_CACHE is not None and not forcar_recarga:
        return BASELINE_CACHE

    registrar_aliases_legados()
    pasta = diretorio_artefatos or LOCAL_ARTIFACTS_DIR

    arquivos_calibrador = sorted(
        pasta.glob("meta_calibrator_*.joblib"),
        key=lambda x: x.stat().st_mtime,
        reverse=True,
    )
    arquivos_conformal = sorted(
        pasta.glob("conformal_artifacts_*.joblib"),
        key=lambda x: x.stat().st_mtime,
        reverse=True,
    )

    if not arquivos_calibrador:
        raise FileNotFoundError(f"Nenhum arquivo meta_calibrator_*.joblib encontrado em {pasta.resolve()}")
    if not arquivos_conformal:
        raise FileNotFoundError(f"Nenhum arquivo conformal_artifacts_*.joblib encontrado em {pasta.resolve()}")

    calibrador: ModeloCalibrador = joblib.load(arquivos_calibrador[0])
    if calibrador.booster is None and getattr(calibrador, "classificador_calibrado", None) is not None:
        clf = calibrador.classificador_calibrado
        if hasattr(clf, "calibrated_classifiers_") and len(clf.calibrated_classifiers_) > 0:
            calibrador.booster = clf.calibrated_classifiers_[0].estimator.booster_
        prep = getattr(calibrador, "preprocessor", None)
        if prep is not None and hasattr(prep, "named_steps"):
            med_arr = prep.named_steps["imputer"].statistics_
            mean_arr = prep.named_steps["scaler"].mean_
            scale_arr = prep.named_steps["scaler"].scale_
            calibrador.medianas_imputacao = dict(
                zip(calibrador.feature_names, [float(x) for x in med_arr], strict=True)
            )
            calibrador.medias_scaler = dict(zip(calibrador.feature_names, [float(x) for x in mean_arr], strict=True))
            calibrador.desvios_scaler = dict(zip(calibrador.feature_names, [float(x) for x in scale_arr], strict=True))
    conformal: ArtefatosConforme = joblib.load(arquivos_conformal[0])
    prompt_sistema = carregar_prompt_sistema()

    BASELINE_CACHE = BaselineReferencia(
        calibrador=calibrador,
        conformal=conformal,
        prompt_sistema=prompt_sistema,
    )
    return BASELINE_CACHE


def preaquecer_modelo_embedding() -> None:
    """Pre-aquece o SentenceTransformer antes do pool de workers."""
    from pipelines.rj_smtr__triagem_recursos.helpers.feature_extractor import obter_modelo_embedding

    try:
        obter_modelo_embedding()
    except Exception as exc:
        logger.debug("Pre-aquecimento de embeddings ignorado nesta inicializacao: %s", exc)


def carregar_artefatos_pipeline(
    is_local: bool = False,
    arquivo_prompt: str = f"{VERSAO_PROMPT_PADRAO}.txt",
    infisical_secret_path: str | None = "/rj_smtr__triagem_recursos",
) -> dict[str, Any]:
    """Carrega calibracao ativa do PostgreSQL (fallback local apenas se is_local=True) e prompt do sistema."""
    from pipelines.rj_smtr__triagem_recursos.helpers.db_repository import buscar_configuracao_calibracao_ativa

    db_url = resolver_banco_url(is_local)
    calibrador = None
    conformal = None
    calibracao_id = None
    versao_modelo = "baseline_congelada_v3"

    try:
        logger.info("Recuperando configuracao de calibracao ativa no PostgreSQL...")
        config_db = buscar_configuracao_calibracao_ativa(db_url)
        calibrador = criar_calibrador_dos_parametros_banco(config_db)
        conformal = criar_artefatos_conforme_do_banco(config_db)
        calibracao_id = config_db.get("calibracao_id")
        versao_modelo = config_db.get("versao_modelo", "db_ativa")
        logger.info(
            "Configuracao ativa do banco carregada | Versao: %s | ID: %s",
            versao_modelo,
            calibracao_id,
        )
    except Exception as exc:
        if not is_local:
            logger.error("Configuracao de calibracao ativa indisponivel no Cloud SQL: %s", exc)
            raise RuntimeError(f"Falha ao carregar configuracao de calibracao ativa no Cloud SQL: {exc}") from exc
        logger.warning(
            "Configuracao no banco local indisponivel (%s). Usando baseline congelada...",
            exc,
        )
        baseline = carregar_baseline_referencia()
        calibrador = baseline.calibrador
        conformal = baseline.conformal

    caminho_prompt = PROMPTS_DIR / arquivo_prompt
    if not caminho_prompt.exists():
        caminho_prompt = PROMPTS_DIR / f"{VERSAO_PROMPT_PADRAO}.txt"

    prompt_sistema = carregar_prompt_sistema(
        caminho_prompt=caminho_prompt,
        infisical_secret_path=infisical_secret_path,
        is_local=is_local,
    )
    versao_prompt = Path(arquivo_prompt).stem or VERSAO_PROMPT_PADRAO
    preaquecer_modelo_embedding()

    return {
        "calibrador": calibrador,
        "conformal": conformal,
        "prompt_sistema": prompt_sistema,
        "calibracao_id": calibracao_id,
        "versao_modelo": versao_modelo,
        "versao_prompt": versao_prompt,
    }
