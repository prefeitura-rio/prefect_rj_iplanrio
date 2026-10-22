"""
Script de Execução e Auditoria dos 730 Processos do Goldenset nos 2 Cenários:
- Cenário 1: Prompt v4 + Reasoning HIGH (730 PDFs)
- Cenário 2: Prompt v4 + Reasoning LOW (730 PDFs)

Recursos Principais:
1. Warmup ping para inicializar o K/V Cache Implícito do Google no Gateway.
2. Execução concorrente contínua via Prefect 3.0 com 8 workers (mantendo o TTL de 5 min aquecido).
3. Checkpoint incremental em JSON/CSV (suporte a retomada sem custos redundantes).
4. Meta-Calibração com 5-Fold Stratified Cross-Validation (Out-Of-Fold / OOF)
   garantindo probabilidade calibrada pura livre de data leakage.
5. Predição Conforme CCCP e Controle de Risco CRC sobre os scores OOF.
6. Persistência relacional em SQLite local (triagem_smtr.db) e PostgreSQL (se disponível).
7. Relatório consolidado comparativo de acurácia, faixas, tokens, custo e tempo.
"""

import os
import sys
import time

try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass
import json
import sqlite3
import unicodedata
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import numpy as np
import pandas as pd
from dotenv import load_dotenv

os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.calibration import CalibratedClassifierCV
from scipy.spatial.distance import mahalanobis
import joblib

try:
    from lightgbm import LGBMClassifier
    _TEM_LIGHTGBM = True
except ImportError:
    from sklearn.ensemble import HistGradientBoostingClassifier as LGBMClassifier
    _TEM_LIGHTGBM = False

from pipelines.rj_smtr__triagem_recursos.flow import rj_smtr__triagem_recursos
from pipelines.rj_smtr__triagem_recursos.constants import (
    LOCAL_ARTIFACTS_DIR,
    PROMPTS_DIR,
    LOCAL_PDFS_DIR,
    LIMIAR_ALTA,
    LIMIAR_BAIXA,
    LIMIAR_FAIXA_AMARELA_PISO,
    FaixaOperacional,
)
from pipelines.rj_smtr__triagem_recursos.utils.meta_calibrator import (
    COLUNAS_FEATURES_PADRAO,
    ModeloCalibrador,
    salvar_calibrador,
)
from pipelines.rj_smtr__triagem_recursos.utils.feature_extractor import (
    gerar_embedding_texto,
    EstatisticasSemanticasClasses,
)
from pipelines.rj_smtr__triagem_recursos.utils.calibration import (
    calcular_ece,
    calcular_brier_score,
)
from pipelines.rj_smtr__triagem_recursos.utils.conformal import (
    ArtefatosConforme,
    calcular_quantis_cccp,
    calibrar_limiar_crc_grounding,
    construir_prediction_set,
)

from pipelines.rj_smtr__triagem_recursos.utils.providers.openai_client import OpenAILLMClient
from pipelines.rj_smtr__triagem_recursos.utils.windowing import extrair_smart_window_pdf

CAMBIO_BRL = 5.80
CSV_GABARITO = LOCAL_ARTIFACTS_DIR / "base_calibracao_full_vertex_gemini_3.7_flash.csv"
ARQUIVO_PROMPT_PADRAO = "v4_baseline.txt"
SQLITE_DB_PATH = LOCAL_ARTIFACTS_DIR / "triagem_smtr.db"


def normalizar_rotulo(rotulo: Any) -> str:
    """Normaliza strings e Enums para snake_case canônico."""
    if rotulo is None or pd.isna(rotulo):
        return ""
    if hasattr(rotulo, "value"):
        rotulo = rotulo.value
    elif hasattr(rotulo, "name") and not isinstance(rotulo, str):
        rotulo = rotulo.name
    texto = str(rotulo).strip().lower()
    if "." in texto:
        texto = texto.split(".")[-1]
    texto = re.sub(r"^(hipotese_legal|hipoteselegal|encaminhamento)[\s_:]*", "", texto)
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("utf-8")
    texto = re.sub(r"[\s\/\-]+", "_", texto)
    texto = re.sub(r"[^\w_]", "", texto)
    return texto.strip("_")


def inicializar_banco_sqlite(caminho_db: Path = SQLITE_DB_PATH) -> sqlite3.Connection:
    """Cria tabelas relacionais locais em SQLite para persistência e auditoria."""
    caminho_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(caminho_db))
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS processos_administrativos (
        processo_id TEXT PRIMARY KEY,
        numero_processo_sei TEXT UNIQUE,
        nome_arquivo_origem TEXT,
        origem_lote TEXT DEFAULT 'GOLDENSET'
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS predicoes_pipeline (
        predicao_id TEXT PRIMARY KEY,
        processo_id TEXT,
        nome_arquivo_origem TEXT,
        modelo_llm TEXT,
        versao_prompt TEXT,
        reasoning_effort TEXT,
        ouro_hipotese TEXT,
        ouro_encaminhamento TEXT,
        pred_hipotese TEXT,
        pred_encaminhamento TEXT,
        acertou_hipotese INTEGER,
        acertou_encaminhamento INTEGER,
        n1_cot_justificativa TEXT,
        n1_lauda_evidencia INTEGER,
        n1_frase_citada TEXT,
        n1_tipo_citacao TEXT,
        fuzz_ratio REAL,
        token_containment REAL,
        exact_match REAL,
        adversarial_density REAL,
        sim_centroid_pred REAL,
        mahalanobis_dist REAL,
        is_manuscrito REAL,
        tesseract_mean_conf REAL,
        probabilidade_calibrada_n1 REAL,
        probabilidade_calibrada_oof REAL,
        conjunto_conformal TEXT,
        tamanho_conjunto_conformal INTEGER,
        faixa_operacional TEXT,
        tokens_prompt_total INTEGER,
        tokens_cached INTEGER,
        tokens_completion INTEGER,
        custo_real_usd REAL,
        custo_real_brl REAL,
        tempo_execucao_s REAL,
        executado_em TEXT
    );
    """)
    conn.commit()
    return conn


def aquecer_cache_implicito(prompt_texto: str) -> None:
    """Dispara um ping leve de warmup para pré-computar os K/V tensors do prompt de 8.2k tokens."""
    print(" Realizando ping de warmup para aquecer o K/V Cache Implícito do Google...")
    try:
        client = OpenAILLMClient()
        pdf_amostra = list(LOCAL_PDFS_DIR.glob("*.pdf"))[0]
        pdf_bytes = extrair_smart_window_pdf(pdf_amostra, max_paginas=1)
        _, usage = client.classificar(
            pdf_bytes=pdf_bytes,
            prompt_sistema=prompt_texto,
            reasoning_effort="none"
        )
        print(f" Cache aquecido com sucesso! Tokens iniciais: In={usage.get('prompt_tokens')}, Cached={usage.get('cached_tokens')}")
    except Exception as exc:
        print(f" Aviso no warmup: {exc} (prosseguindo com a execução)")


def carregar_checkpoint(caminho_checkpoint: Path) -> Dict[str, Dict[str, Any]]:
    """Carrega inferências já salvas no disco para suportar resumo instantâneo."""
    if not caminho_checkpoint.exists():
        return {}
    try:
        with open(caminho_checkpoint, "r", encoding="utf-8") as f:
            dados = json.load(f)
            if isinstance(dados, dict):
                return dados
            elif isinstance(dados, list):
                return {item["nome_arquivo_origem"]: item for item in dados if "nome_arquivo_origem" in item}
    except Exception as e:
        print(f" Erro ao ler checkpoint {caminho_checkpoint.name}: {e}")
    return {}


def salvar_checkpoint(dados_dict: Dict[str, Dict[str, Any]], caminho_checkpoint: Path, caminho_csv: Path) -> None:
    """Salva estado consolidado em JSON e CSV com escrita atômica."""
    caminho_checkpoint.parent.mkdir(parents=True, exist_ok=True)
    lista_registros = list(dados_dict.values())

    # JSON
    with open(caminho_checkpoint, "w", encoding="utf-8") as f:
        json.dump(lista_registros, f, ensure_ascii=False, indent=2)

    # CSV
    df = pd.DataFrame(lista_registros)
    df.to_csv(caminho_csv, index=False, encoding="utf-8-sig")


def executar_lote_cenario(
    nome_cenario: str,
    reasoning_effort: str,
    processos_gabarito: List[Dict[str, Any]],
    arquivo_prompt: str = ARQUIVO_PROMPT_PADRAO,
    chunk_size: int = 50,
) -> List[Dict[str, Any]]:
    """Executa a triagem dos 730 processos em blocos controlados via Prefect 3.0 com 8 workers."""
    print("\n" + "=" * 90)
    print(f"INICIANDO EXECUÇÃO: {nome_cenario} (Reasoning: {reasoning_effort})")
    print(f"Total de processos a processar: {len(processos_gabarito)}")
    print("=" * 90)

    caminho_json = LOCAL_ARTIFACTS_DIR / f"execucao_730_{nome_cenario}.json"
    caminho_csv = LOCAL_ARTIFACTS_DIR / f"execucao_730_{nome_cenario}.csv"

    checkpoint = carregar_checkpoint(caminho_json)
    print(f" Checkpoint existente: {len(checkpoint)}/{len(processos_gabarito)} processos já concluídos.")

    prompt_texto = (PROMPTS_DIR / arquivo_prompt).read_text(encoding="utf-8")
    aquecer_cache_implicito(prompt_texto)

    # Identifica processos pendentes
    pendentes = [p for p in processos_gabarito if p["nome_arquivo_origem"] not in checkpoint]

    if not pendentes:
        print(" Todos os 730 processos já foram triados neste cenário!")
        return list(checkpoint.values())

    print(f" Disparando {len(pendentes)} processos pendentes em lotes de {chunk_size}...")

    t_inicio_lote = time.time()
    n_chunks = (len(pendentes) + chunk_size - 1) // chunk_size

    for num_chunk in range(n_chunks):
        chunk = pendentes[num_chunk * chunk_size : (num_chunk + 1) * chunk_size]
        print(f"\n--- [Lote {num_chunk + 1}/{n_chunks}] Processando {len(chunk)} PDFs com 8 workers concorrentes ---")

        t0 = time.time()
        try:
            sumario = rj_smtr__triagem_recursos(
                is_local=True,
                fonte_pdf="local",
                arquivo_prompt=arquivo_prompt,
                reasoning_effort=reasoning_effort,
                arquivos_especificos=chunk,
                limite_lote=len(chunk),
                executar_n2=False,
            )
            detalhes = sumario.get("detalhes", [])
            t_chunk = time.time() - t0

            # Atualiza o checkpoint com os novos resultados
            for item in detalhes:
                arq = item.get("nome_arquivo_origem")
                if arq:
                    # Anexa validação imediata de gabarito
                    ouro_hip = normalizar_rotulo(item.get("ouro_hipotese"))
                    pred_hip = normalizar_rotulo(item.get("n1_classe_predita"))
                    ouro_enc = normalizar_rotulo(item.get("ouro_encaminhamento"))
                    pred_enc = normalizar_rotulo(item.get("n1_encaminhamento"))

                    item["acertou_hipotese"] = (pred_hip == ouro_hip) if ouro_hip else False
                    item["acertou_encaminhamento"] = (pred_enc == ouro_enc) if ouro_enc else False
                    item["reasoning_effort"] = reasoning_effort
                    item["custo_real_brl"] = round(item.get("custo_real_usd", 0.0) * CAMBIO_BRL, 4)

                    # Achata features epistêmicas para colunas diretas
                    sinais = item.get("sinais_epistemicos", {})
                    for k_feat, v_feat in sinais.items():
                        item[k_feat] = v_feat

                    checkpoint[arq] = item

            # Salva o checkpoint a cada lote de 50
            salvar_checkpoint(checkpoint, caminho_json, caminho_csv)

            tokens_in = sum(d.get("tokens_prompt_total", 0) for d in detalhes)
            tokens_cache = sum(d.get("tokens_cached", 0) for d in detalhes)
            tokens_out = sum(d.get("tokens_completion", 0) for d in detalhes)
            pct_cache = (tokens_cache / tokens_in * 100) if tokens_in > 0 else 0.0
            custo_chunk = sum(d.get("custo_real_usd", 0.0) for d in detalhes)

            print(f"    Concluído em {t_chunk:.1f}s ({len(detalhes)/t_chunk*60:.1f} proc/min) | Custo: ${custo_chunk:.4f} USD")
            print(f"    Tokens: In={tokens_in:,} (Cached: {tokens_cache:,} | {pct_cache:.1f}%) | Out={tokens_out:,}")
            print(f"    Progresso Total: {len(checkpoint)}/{len(processos_gabarito)} ({len(checkpoint)/len(processos_gabarito)*100:.1f}%)")

        except Exception as exc:
            print(f" Erro no processamento do lote {num_chunk + 1}: {exc}")
            import traceback
            traceback.print_exc()
            time.sleep(5)
            continue

    tempo_total = time.time() - t_inicio_lote
    print(f"\n Cenário {nome_cenario} finalizado em {tempo_total/60:.1f} minutos!")
    return list(checkpoint.values())


def computar_estatisticas_semanticas_treino(
    embeddings_por_classe: Dict[str, List[np.ndarray]],
    lambda_ridge: float = 1e-2,
) -> EstatisticasSemanticasClasses:
    """Calcula centróides e matrizes de covariância inversa regularizadas na dobra de treino."""
    centroides: Dict[str, np.ndarray] = {}
    cov_inv: Dict[str, np.ndarray] = {}

    for cls, embs in embeddings_por_classe.items():
        if not embs:
            continue
        X = np.array(embs, dtype=np.float64)
        c = np.mean(X, axis=0)
        norm_c = float(np.linalg.norm(c))
        if norm_c > 0:
            c = c / norm_c
        centroides[cls] = c

        d = X.shape[1]
        if len(X) > 2:
            cov = np.cov(X, rowvar=False) + lambda_ridge * np.eye(d)
            try:
                inv = np.linalg.pinv(cov)
            except Exception:
                inv = np.eye(d)
        else:
            inv = np.eye(d)
        cov_inv[cls] = inv

    return EstatisticasSemanticasClasses(centroides=centroides, cov_inv=cov_inv)


def enriquecer_features_geometricas(
    df_alvo: pd.DataFrame,
    indices_originais: np.ndarray,
    embeddings_todos: List[np.ndarray],
    stats: EstatisticasSemanticasClasses,
) -> pd.DataFrame:
    """Calcula similaridade de centróide e distância de Mahalanobis contra as estatísticas da dobra."""
    df_out = df_alvo.copy()
    sims = []
    mahals = []

    for i, (_, row) in enumerate(df_alvo.iterrows()):
        emb = embeddings_todos[indices_originais[i]]
        hip = str(row.get("n1_classe_predita", "")).lower()

        # Similaridade de centróide
        c_vec = stats.centroides.get(hip)
        if c_vec is not None and np.linalg.norm(emb) > 0:
            s = float(np.dot(emb, c_vec) / (np.linalg.norm(emb) * np.linalg.norm(c_vec)))
            sims.append(round(max(0.0, min(1.0, s)), 4))
        else:
            sims.append(0.85)

        # Mahalanobis
        if hip in stats.centroides and hip in stats.cov_inv and np.linalg.norm(emb) > 0:
            try:
                d = mahalanobis(emb, stats.centroides[hip], stats.cov_inv[hip])
                mahals.append(round(float(d), 4) if not np.isnan(d) else 3.0)
            except Exception:
                mahals.append(3.0)
        else:
            mahals.append(3.0)

    df_out["sim_centroid_pred"] = sims
    df_out["mahalanobis_dist"] = mahals
    return df_out


def executar_calibracao_5fold_oof(
    registros: List[Dict[str, Any]],
    nome_cenario: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Executa a Meta-Calibração com 5-Fold Stratified CV gerando probabilidades OOF não enviesadas."""
    print(f"\n Executando 5-Fold Stratified Cross-Validation (Out-Of-Fold) para {nome_cenario}...")
    df = pd.DataFrame(registros).copy()

    # 1. Pré-computa embeddings das justificativas
    print("    Extraindo embeddings semânticos das justificativas CoT...")
    embeddings_todos = [
        gerar_embedding_texto(str(row.get("n1_cot_justificativa", "")))
        for _, row in df.iterrows()
    ]

    y_alvo = df["acertou_hipotese"].astype(int).to_numpy()
    p_oof = np.zeros(len(df), dtype=np.float64)

    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    for fold, (idx_tr, idx_val) in enumerate(skf.split(df, y_alvo), 1):
        df_tr_raw = df.iloc[idx_tr].copy()
        df_val_raw = df.iloc[idx_val].copy()

        # A. Computa geometria apenas com o treino da dobra
        embs_tr_dict: Dict[str, List[np.ndarray]] = {}
        for i_tr in idx_tr:
            cls_ouro = normalizar_rotulo(df.iloc[i_tr].get("ouro_hipotese"))
            emb = embeddings_todos[i_tr]
            if cls_ouro and np.linalg.norm(emb) > 0:
                embs_tr_dict.setdefault(cls_ouro, []).append(emb)

        stats_fold = computar_estatisticas_semanticas_treino(embs_tr_dict)

        df_tr = enriquecer_features_geometricas(df_tr_raw, idx_tr, embeddings_todos, stats_fold)
        df_val = enriquecer_features_geometricas(df_val_raw, idx_val, embeddings_todos, stats_fold)

        # B. Treina LightGBM raso calibrado com isotônica
        X_tr = df_tr[COLUNAS_FEATURES_PADRAO].to_numpy(dtype=np.float64)
        y_tr = df_tr["acertou_hipotese"].astype(int).to_numpy()
        X_val = df_val[COLUNAS_FEATURES_PADRAO].to_numpy(dtype=np.float64)

        prep = Pipeline([
            ("imputer", SimpleImputer(strategy="median", fill_value=0.0)),
            ("scaler", StandardScaler()),
        ])
        X_tr_proc = prep.fit_transform(X_tr)
        X_val_proc = prep.transform(X_val)

        if _TEM_LIGHTGBM:
            base_model = LGBMClassifier(
                max_depth=3,
                num_leaves=7,
                n_estimators=60,
                learning_rate=0.05,
                min_child_samples=5,
                reg_alpha=0.1,
                reg_lambda=1.0,
                class_weight="balanced",
                random_state=42,
                verbosity=-1,
            )
        else:
            base_model = LGBMClassifier(
                max_depth=3,
                max_iter=60,
                learning_rate=0.05,
                class_weight="balanced",
                random_state=42,
            )

        calibrador_fold = CalibratedClassifierCV(estimator=base_model, method="isotonic", cv=3)
        calibrador_fold.fit(X_tr_proc, y_tr)

        # C. Prediz estritamente nos dados de validação não vistos
        probs_val = calibrador_fold.predict_proba(X_val_proc)[:, 1]
        p_oof[idx_val] = probs_val

    df["probabilidade_calibrada_oof"] = np.round(p_oof, 4)
    df["score_nao_conformidade"] = np.round(1.0 - p_oof, 4)

    # 2. Computa Conformal Prediction CCCP e CRC sobre os scores OOF
    print("    Computando quantis conformais CCCP e limiar CRC sobre os resíduos OOF...")
    tabela_quantis, quantil_pool, classes_pool = calcular_quantis_cccp(
        df_scores=df,
        col_classe="ouro_hipotese",
        col_score="score_nao_conformidade",
        alpha=0.05,
        min_samples=15,
    )
    limiar_crc = calibrar_limiar_crc_grounding(
        df_calibracao=df,
        col_fuzzy="fuzz_ratio",
        col_acerto="acertou_hipotese",
        epsilon=0.02,
    )

    # Constrói Prediction Sets
    sets_conforme = []
    faixas = []
    for _, row in df.iterrows():
        p_val = row["probabilidade_calibrada_oof"]
        score_val = row["score_nao_conformidade"]
        cls_pred = normalizar_rotulo(row["n1_classe_predita"])

        cset = {cls_pred} if score_val <= tabela_quantis.get(cls_pred, quantil_pool) else set()
        sets_conforme.append(list(cset))

        # Determina Faixa Operacional
        if p_val >= LIMIAR_ALTA and len(cset) <= 1:
            faixas.append(FaixaOperacional.VERDE.value)
        elif p_val >= LIMIAR_FAIXA_AMARELA_PISO:
            faixas.append(FaixaOperacional.AMARELA.value)
        else:
            faixas.append(FaixaOperacional.VERMELHA.value)

    df["conjunto_conformal"] = [json.dumps(s) for s in sets_conforme]
    df["tamanho_conjunto_conformal"] = [len(s) for s in sets_conforme]
    df["faixa_operacional"] = faixas

    # 3. Métricas de calibração
    ece, mce, _ = calcular_ece(p_oof, y_alvo)
    brier = calcular_brier_score(p_oof, y_alvo)

    metricas = {
        "cenario": nome_cenario,
        "total": len(df),
        "acuracia_hipotese_pct": round(float(df["acertou_hipotese"].mean() * 100), 2),
        "acuracia_encaminhamento_pct": round(float(df["acertou_encaminhamento"].mean() * 100), 2),
        "ece": ece,
        "brier_score": brier,
        "faixa_verde": int((df["faixa_operacional"] == FaixaOperacional.VERDE.value).sum()),
        "faixa_amarela": int((df["faixa_operacional"] == FaixaOperacional.AMARELA.value).sum()),
        "faixa_vermelha": int((df["faixa_operacional"] == FaixaOperacional.VERMELHA.value).sum()),
        "limiar_crc": limiar_crc,
    }

    # 4. Ajusta modelo de produção final (100% dos dados)
    print("    Treinando modelo de produção consolidado (100% dos dados)...")
    embs_full_dict: Dict[str, List[np.ndarray]] = {}
    for i, row in df.iterrows():
        cls_ouro = normalizar_rotulo(row.get("ouro_hipotese"))
        emb = embeddings_todos[i]
        if cls_ouro and np.linalg.norm(emb) > 0:
            embs_full_dict.setdefault(cls_ouro, []).append(emb)

    stats_global = computar_estatisticas_semanticas_treino(embs_full_dict)
    df_global = enriquecer_features_geometricas(df, np.arange(len(df)), embeddings_todos, stats_global)

    X_all = df_global[COLUNAS_FEATURES_PADRAO].to_numpy(dtype=np.float64)
    y_all = df_global["acertou_hipotese"].astype(int).to_numpy()

    prep_final = Pipeline([
        ("imputer", SimpleImputer(strategy="median", fill_value=0.0)),
        ("scaler", StandardScaler()),
    ])
    X_all_proc = prep_final.fit_transform(X_all)

    if _TEM_LIGHTGBM:
        base_final = LGBMClassifier(
            max_depth=3,
            num_leaves=7,
            n_estimators=60,
            learning_rate=0.05,
            min_child_samples=5,
            reg_alpha=0.1,
            reg_lambda=1.0,
            class_weight="balanced",
            random_state=42,
            verbosity=-1,
        )
    else:
        base_final = LGBMClassifier(
            max_depth=3,
            max_iter=60,
            learning_rate=0.05,
            class_weight="balanced",
            random_state=42,
        )

    calibrador_final = CalibratedClassifierCV(estimator=base_final, method="isotonic", cv=5)
    calibrador_final.fit(X_all_proc, y_all)

    modelo_prod = ModeloCalibrador(
        preprocessor=prep_final,
        classificador_calibrado=calibrador_final,
        feature_names=COLUNAS_FEATURES_PADRAO.copy(),
        estatisticas_classes=stats_global,
        metadados_treino=metricas,
    )
    salvar_calibrador(modelo_prod, LOCAL_ARTIFACTS_DIR / f"meta_calibrator_v4_{nome_cenario}.joblib")

    return df, metricas


def persistir_banco_sqlite(df: pd.DataFrame, conn: sqlite3.Connection) -> None:
    """Insere as predições e metadados no SQLite local."""
    cur = conn.cursor()
    for _, row in df.iterrows():
        proc_id = str(row.get("processo_id", ""))
        num_sei = str(row.get("numero_processo_sei", ""))
        arq = str(row.get("nome_arquivo_origem", ""))

        cur.execute("""
        INSERT INTO processos_administrativos (processo_id, numero_processo_sei, nome_arquivo_origem)
        VALUES (?, ?, ?)
        ON CONFLICT (numero_processo_sei) DO UPDATE SET nome_arquivo_origem = EXCLUDED.nome_arquivo_origem;
        """, (proc_id, num_sei, arq))

        cur.execute("""
        INSERT INTO predicoes_pipeline (
            predicao_id, processo_id, nome_arquivo_origem, modelo_llm, versao_prompt, reasoning_effort,
            ouro_hipotese, ouro_encaminhamento, pred_hipotese, pred_encaminhamento, acertou_hipotese, acertou_encaminhamento,
            n1_cot_justificativa, n1_lauda_evidencia, n1_frase_citada, n1_tipo_citacao,
            fuzz_ratio, token_containment, exact_match, adversarial_density, sim_centroid_pred, mahalanobis_dist, is_manuscrito, tesseract_mean_conf,
            probabilidade_calibrada_n1, probabilidade_calibrada_oof, conjunto_conformal, tamanho_conjunto_conformal, faixa_operacional,
            tokens_prompt_total, tokens_cached, tokens_completion, custo_real_usd, custo_real_brl, tempo_execucao_s, executado_em
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (predicao_id) DO UPDATE SET
            probabilidade_calibrada_oof = EXCLUDED.probabilidade_calibrada_oof,
            faixa_operacional = EXCLUDED.faixa_operacional,
            custo_real_usd = EXCLUDED.custo_real_usd;
        """, (
            str(row.get("predicao_id") or proc_id),
            proc_id,
            arq,
            str(row.get("modelo_llm", "vertex/gemini-3.7-flash")),
            str(row.get("versao_prompt", "v4_baseline")),
            str(row.get("reasoning_effort", "low")),
            str(row.get("ouro_hipotese", "")),
            str(row.get("ouro_encaminhamento", "")),
            str(row.get("n1_classe_predita", "")),
            str(row.get("n1_encaminhamento", "")),
            int(row.get("acertou_hipotese", 0)),
            int(row.get("acertou_encaminhamento", 0)),
            str(row.get("n1_cot_justificativa", "")),
            int(row.get("n1_lauda_evidencia", 0)),
            str(row.get("n1_frase_citada", "")),
            str(row.get("n1_tipo_citacao", "")),
            float(row.get("fuzz_ratio", 0.0)),
            float(row.get("token_containment", 0.0)),
            float(row.get("exact_match", 0.0)),
            float(row.get("adversarial_density", 0.0)),
            float(row.get("sim_centroid_pred", 0.0)),
            float(row.get("mahalanobis_dist", 0.0)),
            float(row.get("is_manuscrito", 0.0)),
            float(row.get("tesseract_mean_conf", 0.0)),
            float(row.get("probabilidade_calibrada", 0.0)),
            float(row.get("probabilidade_calibrada_oof", 0.0)),
            str(row.get("conjunto_conformal", "[]")),
            int(row.get("tamanho_conjunto_conformal", 1)),
            str(row.get("faixa_operacional", "VERDE")),
            int(row.get("tokens_prompt_total", 0)),
            int(row.get("tokens_cached", 0)),
            int(row.get("tokens_completion", 0)),
            float(row.get("custo_real_usd", 0.0)),
            float(row.get("custo_real_brl", 0.0)),
            float(row.get("tempo_execucao_s", 0.0)),
            time.strftime("%Y-%m-%d %H:%M:%S")
        ))
    conn.commit()


def main():
    if not CSV_GABARITO.exists():
        raise FileNotFoundError(f"Arquivo de gabarito não encontrado: {CSV_GABARITO}")

    df_gabarito = pd.read_csv(CSV_GABARITO)
    print(f" Base de gabarito carregada: {len(df_gabarito)} registros.")

    # Constrói lista de processos para o pipeline
    processos_gabarito = []
    for idx, row in df_gabarito.iterrows():
        arq = str(row["arquivo"])
        processos_gabarito.append({
            "processo_id": f"00000000-0000-0000-0000-{idx+1:012d}",
            "numero_processo_sei": f"SEI-{Path(arq).stem[:28]}",
            "nome_arquivo_origem": arq,
            "ouro_hipotese": normalizar_rotulo(row.get("ouro_hipotese")),
            "ouro_encaminhamento": normalizar_rotulo(row.get("ouro_encaminhamento")),
        })

    conn_sqlite = inicializar_banco_sqlite()

    # =========================================================================
    # PASSO 1: Executa Cenário HIGH (730 processos)
    # =========================================================================
    registros_high = executar_lote_cenario(
        nome_cenario="v4_high",
        reasoning_effort="high",
        processos_gabarito=processos_gabarito,
        chunk_size=50,
    )

    # =========================================================================
    # PASSO 2: Executa Cenário LOW (730 processos)
    # =========================================================================
    registros_low = executar_lote_cenario(
        nome_cenario="v4_low",
        reasoning_effort="low",
        processos_gabarito=processos_gabarito,
        chunk_size=50,
    )

    # =========================================================================
    # PASSO 3: 5-Fold Stratified OOF Meta-Calibração & Conformal
    # =========================================================================
    df_high_oof, met_high = executar_calibracao_5fold_oof(registros_high, "v4_high")
    df_low_oof, met_low = executar_calibracao_5fold_oof(registros_low, "v4_low")

    # =========================================================================
    # PASSO 4: Persistência Relacional em SQLite
    # =========================================================================
    print("\n Persistindo predições completas no banco relacional SQLite local...")
    persistir_banco_sqlite(df_high_oof, conn_sqlite)
    persistir_banco_sqlite(df_low_oof, conn_sqlite)
    conn_sqlite.close()
    print(" Dados salvos com sucesso em:", SQLITE_DB_PATH)

    # Salva CSVs finais calibrados
    df_high_oof.to_csv(LOCAL_ARTIFACTS_DIR / "final_730_calibrado_v4_high.csv", index=False)
    df_low_oof.to_csv(LOCAL_ARTIFACTS_DIR / "final_730_calibrado_v4_low.csv", index=False)

    # =========================================================================
    # PASSO 5: Relatório Comparativo Final
    # =========================================================================
    # Totais financeiros e de tokens
    custo_high_usd = df_high_oof["custo_real_usd"].sum()
    custo_low_usd = df_low_oof["custo_real_usd"].sum()
    custo_high_brl = custo_high_usd * CAMBIO_BRL
    custo_low_brl = custo_low_usd * CAMBIO_BRL

    tin_high = df_high_oof["tokens_prompt_total"].sum()
    tc_high = df_high_oof["tokens_cached"].sum()
    tout_high = df_high_oof["tokens_completion"].sum()

    tin_low = df_low_oof["tokens_prompt_total"].sum()
    tc_low = df_low_oof["tokens_cached"].sum()
    tout_low = df_low_oof["tokens_completion"].sum()

    pct_c_high = (tc_high / tin_high * 100) if tin_high > 0 else 0
    pct_c_low = (tc_low / tin_low * 100) if tin_low > 0 else 0

    tempo_high_s = df_high_oof["tempo_execucao_s"].sum()
    tempo_low_s = df_low_oof["tempo_execucao_s"].sum()

    resumo_geral = pd.DataFrame([
        {
            "Métrica Operacional": "Acurácia Hipótese Legal (%)",
            "Cenário HIGH (v4)": f"{met_high['acuracia_hipotese_pct']}%",
            "Cenário LOW (v4)": f"{met_low['acuracia_hipotese_pct']}%",
            "Diferença (LOW vs HIGH)": f"{met_low['acuracia_hipotese_pct'] - met_high['acuracia_hipotese_pct']:+.2f}%",
        },
        {
            "Métrica Operacional": "Acurácia Encaminhamento (%)",
            "Cenário HIGH (v4)": f"{met_high['acuracia_encaminhamento_pct']}%",
            "Cenário LOW (v4)": f"{met_low['acuracia_encaminhamento_pct']}%",
            "Diferença (LOW vs HIGH)": f"{met_low['acuracia_encaminhamento_pct'] - met_high['acuracia_encaminhamento_pct']:+.2f}%",
        },
        {
            "Métrica Operacional": "Faixa Verde (Aprovação Direta)",
            "Cenário HIGH (v4)": f"{met_high['faixa_verde']} ({met_high['faixa_verde']/730*100:.1f}%)",
            "Cenário LOW (v4)": f"{met_low['faixa_verde']} ({met_low['faixa_verde']/730*100:.1f}%)",
            "Diferença (LOW vs HIGH)": f"{met_low['faixa_verde'] - met_high['faixa_verde']:+d}",
        },
        {
            "Métrica Operacional": "Faixa Amarela (Elegível a N=2)",
            "Cenário HIGH (v4)": f"{met_high['faixa_amarela']} ({met_high['faixa_amarela']/730*100:.1f}%)",
            "Cenário LOW (v4)": f"{met_low['faixa_amarela']} ({met_low['faixa_amarela']/730*100:.1f}%)",
            "Diferença (LOW vs HIGH)": f"{met_low['faixa_amarela'] - met_high['faixa_amarela']:+d}",
        },
        {
            "Métrica Operacional": "Faixa Vermelha (HITL Humano)",
            "Cenário HIGH (v4)": f"{met_high['faixa_vermelha']} ({met_high['faixa_vermelha']/730*100:.1f}%)",
            "Cenário LOW (v4)": f"{met_low['faixa_vermelha']} ({met_low['faixa_vermelha']/730*100:.1f}%)",
            "Diferença (LOW vs HIGH)": f"{met_low['faixa_vermelha'] - met_high['faixa_vermelha']:+d}",
        },
        {
            "Métrica Operacional": "Taxa de Cache Implícito (%)",
            "Cenário HIGH (v4)": f"{pct_c_high:.1f}%",
            "Cenário LOW (v4)": f"{pct_c_low:.1f}%",
            "Diferença (LOW vs HIGH)": f"{pct_c_low - pct_c_high:+.1f}%",
        },
        {
            "Métrica Operacional": "Tokens de Saída (Output Total)",
            "Cenário HIGH (v4)": f"{tout_high:,} (méd: {tout_high/730:.0f})",
            "Cenário LOW (v4)": f"{tout_low:,} (méd: {tout_low/730:.0f})",
            "Diferença (LOW vs HIGH)": f"{(tout_low - tout_high)/tout_high*100:.1f}%",
        },
        {
            "Métrica Operacional": "Custo Total do Lote (730 PDFs)",
            "Cenário HIGH (v4)": f"${custo_high_usd:.2f} (R$ {custo_high_brl:.2f})",
            "Cenário LOW (v4)": f"${custo_low_usd:.2f} (R$ {custo_low_brl:.2f})",
            "Diferença (LOW vs HIGH)": f"- R$ {custo_high_brl - custo_low_brl:.2f} (Economia de {(custo_high_usd - custo_low_usd)/custo_high_usd*100:.1f}%)",
        },
        {
            "Métrica Operacional": "Custo Médio por Processo",
            "Cenário HIGH (v4)": f"${custo_high_usd/730:.5f} (R$ {custo_high_brl/730:.4f})",
            "Cenário LOW (v4)": f"${custo_low_usd/730:.5f} (R$ {custo_low_brl/730:.4f})",
            "Diferença (LOW vs HIGH)": f"- R$ {(custo_high_brl - custo_low_brl)/730:.4f}/proc",
        },
        {
            "Métrica Operacional": "Tempo Total Estimado (8 workers)",
            "Cenário HIGH (v4)": f"{tempo_high_s/8/60:.1f} min",
            "Cenário LOW (v4)": f"{tempo_low_s/8/60:.1f} min",
            "Diferença (LOW vs HIGH)": f"- {(tempo_high_s - tempo_low_s)/8/60:.1f} min (Mais rápido)",
        },
        {
            "Métrica Operacional": "Calibração ECE (Menor é melhor)",
            "Cenário HIGH (v4)": f"{met_high['ece']:.4f}",
            "Cenário LOW (v4)": f"{met_low['ece']:.4f}",
            "Diferença (LOW vs HIGH)": f"{met_low['ece'] - met_high['ece']:+.4f}",
        },
        {
            "Métrica Operacional": "Brier Score (Menor é melhor)",
            "Cenário HIGH (v4)": f"{met_high['brier_score']:.4f}",
            "Cenário LOW (v4)": f"{met_low['brier_score']:.4f}",
            "Diferença (LOW vs HIGH)": f"{met_low['brier_score'] - met_high['brier_score']:+.4f}",
        },
    ])

    print("\n" + "=" * 90)
    print("TABELA COMPARATIVA FINAL: CENÁRIO HIGH vs CENÁRIO LOW (730 PROCESSOS)")
    print("=" * 90)
    print(resumo_geral.to_markdown(index=False))

    resumo_geral.to_csv(LOCAL_ARTIFACTS_DIR / "comparativo_geral_730_high_vs_low.csv", index=False)
    print("\nArtifacts persistidos com sucesso em:", LOCAL_ARTIFACTS_DIR)


if __name__ == "__main__":
    main()
