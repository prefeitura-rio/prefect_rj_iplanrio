"""Script de ingestão do Goldenset (730 casos) e gravação de parâmetros no PostgreSQL.

Extrai parâmetros estatísticos puros (JSONB) e o modelo textual do LightGBM,
eliminando dependências de deserialização binária (pickle/joblib).
"""

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List
import lightgbm as lgb
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sklearn.calibration import CalibratedClassifierCV
from sklearn.impute import SimpleImputer
from sklearn.isotonic import IsotonicRegression
from sklearn.preprocessing import StandardScaler

PIPELINE_DIR = Path(__file__).resolve().parent.parent

try:
  from dotenv import find_dotenv, load_dotenv

  load_dotenv(find_dotenv(usecwd=True))
except ImportError:
  pass

from constants import (
    ALPHA_CONFORMAL,
    EPSILON_CRC,
    FEATURES_MODELO_PRODUCAO,
    resolver_banco_url,
)

# Caminhos padrão para localização da base histórica
DATA_CANDIDATE_PATHS = [
    PIPELINE_DIR.parent.parent
    / "artifacts"
    / "v3-gemini-3.7-flash"
    / "base_calibracao_full_vertex_gemini_3.7_flash.csv",
    PIPELINE_DIR / "artifacts" / "base_calibracao_full.csv",
    PIPELINE_DIR.parent.parent / "data" / "goldenset.csv",
]


def localizar_arquivo_base(caminho_informado: str | None) -> Path:
  if caminho_informado:
    p = Path(caminho_informado)
    if p.exists():
      return p
    raise FileNotFoundError(f"Arquivo indicado não encontrado: {p}")

  for candidato in DATA_CANDIDATE_PATHS:
    if candidato.exists():
      return candidato

  # Se não encontrar nos caminhos padrões, busca recursiva por arquivo base_calibracao
  matches = list(PIPELINE_DIR.parent.parent.glob("**/base_calibracao_full*.csv"))
  if matches:
    return matches[0]

  raise FileNotFoundError(
      "Não foi possível localizar o CSV da base de calibração (730 casos)."
      " Informe o caminho via --csv-path."
  )


def extrair_quantis_conforme(
    df: pd.DataFrame, alpha: float = 0.05
) -> tuple[dict[str, float], float]:
  """Calcula a tabela de quantis de não-conformidade por classe (CCCP)."""
  col_classe = (
      "ouro_hipotese" if "ouro_hipotese" in df.columns else "classe_gabarito"
  )
  col_score = (
      "score_nao_conformidade"
      if "score_nao_conformidade" in df.columns
      else "score_confianca"
  )

  tabela_quantis: dict[str, float] = {}
  scores_globais = []

  for classe, grupo in df.groupby(col_classe):
    classe_str = str(classe).strip().lower()
    scores = grupo[col_score].dropna().to_numpy(dtype=np.float64)
    if len(scores) == 0:
      continue
    scores_globais.extend(scores.tolist())

    n = len(scores)
    idx = int(np.ceil((n + 1) * (1.0 - alpha))) - 1
    idx = min(max(0, idx), n - 1)
    q_k = float(np.sort(scores)[idx])
    tabela_quantis[classe_str] = round(q_k, 4)

  # Fallback global
  n_total = len(scores_globais)
  idx_fallback = int(np.ceil((n_total + 1) * (1.0 - alpha))) - 1
  idx_fallback = min(max(0, idx_fallback), n_total - 1)
  quantil_fallback = (
      round(float(np.sort(scores_globais)[idx_fallback]), 4)
      if scores_globais
      else 1.0
  )

  return tabela_quantis, quantil_fallback


def main():
  parser = argparse.ArgumentParser(
      description="Ingestão do Goldenset e extração de parâmetros de calibração."
  )
  parser.add_argument(
      "--local",
      action="store_true",
      default=True,
      help="Executa no banco local Docker.",
  )
  parser.add_argument(
      "--cloud",
      action="store_false",
      dest="local",
      help="Executa no Cloud SQL.",
  )
  parser.add_argument(
      "--csv-path",
      type=str,
      default=None,
      help="Caminho opcional para o CSV dos 730 casos.",
  )
  args = parser.parse_args()

  db_url = resolver_banco_url(is_local=args.local)
  print(f"[1/4] Conectando ao banco de dados: {db_url.split('@')[-1]}")

  # 1. Carrega o CSV base de 730 casos
  csv_file = localizar_arquivo_base(args.csv_path)
  print(f"[2/4] Carregando dados auditados de: {csv_file}")
  df = pd.read_csv(csv_file)

  # Normalização de nomes de colunas
  mapa_colunas = {
      "processo": "numero_processo_sei",
      "processo_sei": "numero_processo_sei",
      "auto_infracao": "numero_auto_infracao",
      "hipotese_ouro": "ouro_hipotese",
      "classe_ouro": "ouro_hipotese",
      "encaminhamento_ouro": "ouro_encaminhamento",
  }
  df = df.rename(columns=mapa_colunas)

  if "numero_processo_sei" not in df.columns:
    df["numero_processo_sei"] = [f"PROC-{i:04d}" for i in range(len(df))]

  # 2. Inserção na tabela goldenset_processos
  print(f"[3/4] Inserindo {len(df)} processos em 'goldenset_processos'...")
  valores_goldenset = []
  for _, row in df.iterrows():
    acertou = bool(
        row.get("acertou_hipotese", row.get("acertou_hipotese_n1", True))
    )
    score_conf = float(
        row.get("score_nao_conformidade", 1.0 - (0.95 if acertou else 0.10))
    )

    valores_goldenset.append((
        str(row["numero_processo_sei"]),
        str(row.get("numero_auto_infracao", "")),
        str(row.get("ouro_hipotese", "hipotese_esgotada")).strip().lower(),
        str(row.get("ouro_encaminhamento", "")),
        "GOLDENSET_INICIAL",
        "SMTR_AUDITORIA_HISTORICA",
        float(row.get("fuzz_ratio", np.nan)),
        float(row.get("token_containment", np.nan)),
        float(row.get("exact_match", np.nan)),
        float(row.get("adversarial_density", np.nan)),
        float(row.get("sim_centroid_pred", np.nan)),
        float(row.get("mahalanobis_dist", np.nan)),
        float(row.get("is_manuscrito", np.nan)),
        float(row.get("tesseract_mean_conf", np.nan)),
        acertou,
        score_conf,
    ))

  query_insert_golden = """
        INSERT INTO goldenset_processos (
            numero_processo_sei, numero_auto_infracao, ouro_hipotese, ouro_encaminhamento,
            origem_validacao, validado_por,
            feat_fuzz_ratio, feat_token_containment, feat_exact_match, feat_adversarial_density,
            feat_sim_centroid_pred, feat_mahalanobis_dist, feat_is_manuscrito, feat_tesseract_mean_conf,
            acertou_hipotese_n1, score_nao_conformidade
        ) VALUES %s
        ON CONFLICT (numero_processo_sei) DO UPDATE SET
            ouro_hipotese = EXCLUDED.ouro_hipotese,
            acertou_hipotese_n1 = EXCLUDED.acertou_hipotese_n1,
            score_nao_conformidade = EXCLUDED.score_nao_conformidade;
    """

  conn = psycopg2.connect(db_url)
  with conn.cursor() as cur:
    execute_values(cur, query_insert_golden, valores_goldenset)
  conn.commit()
  print("      Processos gravados com sucesso.")

  # 3. Extração dos parâmetros estatísticos puros
  print(
      "[4/4] Calculando parâmetros matemáticos puros e treinando LightGBM..."
  )
  colunas_feat = FEATURES_MODELO_PRODUCAO
  X_raw = df[colunas_feat].to_numpy(dtype=np.float64)
  y = df["acertou_hipotese"].astype(int).to_numpy(dtype=int)

  # Imputer
  imputer = SimpleImputer(strategy="median", fill_value=0.0)
  X_imp = imputer.fit_transform(X_raw)
  medianas_dict = {
      feat: float(imputer.statistics_[i]) for i, feat in enumerate(colunas_feat)
  }

  # Scaler
  scaler = StandardScaler()
  X_scaled = scaler.fit_transform(X_imp)
  medias_dict = {
      feat: float(scaler.mean_[i]) for i, feat in enumerate(colunas_feat)
  }
  desvios_dict = {
      feat: float(scaler.scale_[i]) for i, feat in enumerate(colunas_feat)
  }

  # Quantis Conformais CCCP
  quantis_cccp, quantil_fallback = extrair_quantis_conforme(
      df, alpha=ALPHA_CONFORMAL
  )

  # Treinamento do LightGBM Booster
  modelo_lgb = lgb.LGBMClassifier(
      max_depth=3,
      num_leaves=7,
      n_estimators=60,
      learning_rate=0.05,
      class_weight="balanced",
      random_state=42,
      verbosity=-1,
  )
  modelo_lgb.fit(X_scaled, y)

  # Calibração Isotônica pura (para interpolação 1D via np.interp)
  probs_raw = modelo_lgb.predict_proba(X_scaled)[:, 1]
  iso = IsotonicRegression(out_of_bounds="clip")
  iso.fit(probs_raw, y)
  probs_calibradas = iso.predict(probs_raw)

  brier = float(np.mean((probs_calibradas - y) ** 2))
  log_loss = float(
      -np.mean(
          y * np.log(np.clip(probs_calibradas, 1e-6, 1.0))
          + (1 - y) * np.log(np.clip(1.0 - probs_calibradas, 1e-6, 1.0))
      )
  )

  # Exportação do modelo LightGBM como texto nativo
  booster_text = modelo_lgb.booster_.model_to_string()

  curva_isotonica = {
      "x": [round(float(v), 5) for v in iso.X_thresholds_],
      "y": [round(float(v), 5) for v in iso.y_thresholds_],
  }

  # Desativa versões anteriores e grava a nova configuração ativa
  with conn.cursor() as cur:
    cur.execute("UPDATE configuracoes_calibracao SET ativo = FALSE;")

    query_insert_config = """
            INSERT INTO configuracoes_calibracao (
                versao_modelo, ativo, total_amostras_treino, brier_score_treino, log_loss_treino,
                medianas_imputacao, medias_scaler, desvios_scaler,
                quantis_conformais_cccp, quantil_pool_fallback, limiar_crc_grounding, alpha_conformal,
                estatisticas_classes, lightgbm_model_text
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
    cur.execute(
        query_insert_config,
        (
            "v3_baseline_gemini_3.7_flash",
            True,
            len(df),
            round(brier, 4),
            round(log_loss, 4),
            json.dumps(medianas_dict),
            json.dumps(medias_dict),
            json.dumps(desvios_dict),
            json.dumps(quantis_cccp),
            quantil_fallback,
            0.85,
            ALPHA_CONFORMAL,
            json.dumps({"curva_isotonica": curva_isotonica}),
            booster_text,
        ),
    )
  conn.commit()
  conn.close()

  print("\n" + "=" * 60)
  print("✅ CONFIGURAÇÃO ATIVA SALVA NO BANCO:")
  print(f"   - Total Amostras: {len(df)}")
  print(f"   - Brier Score: {brier:.4f} | Log Loss: {log_loss:.4f}")
  print(f"   - Classes CCCP mapeadas: {list(quantis_cccp.keys())}")
  print("   - Modelo LightGBM: Serializado em texto puro (imune a versões)")
  print("=" * 60)


if __name__ == "__main__":
  main()