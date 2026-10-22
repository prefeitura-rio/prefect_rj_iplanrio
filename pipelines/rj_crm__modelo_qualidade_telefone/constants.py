"""Constants of rj_crm__modelo_qualidade_telefone."""

from typing import Final

QUERIES_ANCHOR: Final = __file__
"""A file of the pipeline folder, from which `render_query` finds `queries/`."""
PROJECT_ID: Final = "rj-crm-registry"
DATASET_ID: Final = "crm_whatsapp"
SCORE_TABLE_ID: Final = "telefone_prob_high_delivery"
EVALUATION_TABLE_ID: Final = "telefone_qualidade_modelo_avaliacao"
"""The long table of the retrain: `data_execucao`, `versao`, `origem`, `algoritmo`, `metrica`, `valor`, `detalhe`."""
MODELS_ROOT: Final = "gs://rj-crm-registry/modelos/qualidade_telefone"
DRIVE_ROOT_ID: Final = "1h4wlF6mq9I7NmVCmR2yn6vycShDKoEpy"
"""The folder `Relatórios de retreino` of the Drive. It needs Editor access for the service account."""
DEFAULT_BATCH_SIZE: Final = 300_000
"""The phones that the scoring holds in memory at one time."""
DEFAULT_TRIALS: Final = 30
DEFAULT_RECENT_DAYS: Final = 30

RANDOM_STATE: Final = 42
FUTURE_WINDOW_DAYS: Final = 30
"""The days after the cut date that make the label of a phone."""
HIGH_DELIVERY_THRESHOLD: Final = 0.90
SYSTEMS: Final = ("agendamento_cadunico", "bcadastro", "cadunico", "ergon", "sms")
DELIVERY_RATE_THRESHOLD: Final = 0.50
"""The past success rate above which the heuristic picks a phone."""
RELIABILITY_RANKING: Final = {
    "cadunico": 0.928,
    "ergon": 0.920,
    "bcadastro": 0.900,
    "agendamento_cadunico": 0.894,
    "sms": 0.853,
}
"""P(phone is HighDelivery | system). It is the fallback of the heuristic."""

NUMERIC_FEATURES: Final = (
    "qtd_disparo_anterior",
    "taxa_sucesso_anterior",
    "dias_desde_ultimo_disparo",
    "dias_desde_ultimo_disparo_sucesso",
    "dias_desde_ultima_resposta",
    "n_sistemas_registrado",
    "telefone_associado_cnpj",
    *(f"qtd_aparicoes_{system}" for system in SYSTEMS),
    *(f"dias_desde_atualizacao_{system}" for system in SYSTEMS),
    "dias_desde_primeiro_registro",
    "qtd_cpfs_associados",
    "qtd_telefones_cpf_recente",
    "qtd_cpfs_associados_cadunico",
    "qtd_familias_associadas",
    "concentracao_familiar",
    "pct_cpfs_menor_idade",
)
DUMMY_GROUPS: Final = {
    "ddd_categoria": tuple(f"ddd_{code}" for code in ("11", "21", "22", "24", "83", "outros")),
    "telefone_qualidade": tuple(f"qualidade_{quality}" for quality in ("INVALIDO", "SUSPEITO", "VALIDO")),
}
"""The dummies that `features_telefone.sql` makes, by the column they come from."""
DUMMY_FEATURES: Final = tuple(column for columns in DUMMY_GROUPS.values() for column in columns)
FLOAT_FEATURES: Final = ("taxa_sucesso_anterior", "concentracao_familiar", "pct_cpfs_menor_idade")
"""The features that are fractions. Every other one is a count, a number of days or a dummy."""
SENTINEL_FEATURES: Final = (
    "taxa_sucesso_anterior",
    "dias_desde_ultimo_disparo",
    "dias_desde_ultimo_disparo_sucesso",
    "dias_desde_ultima_resposta",
    *(f"dias_desde_atualizacao_{system}" for system in SYSTEMS),
    "dias_desde_primeiro_registro",
    "qtd_telefones_cpf_recente",
    "concentracao_familiar",
    "pct_cpfs_menor_idade",
)
"""The features whose `-1` means no data. For the others, `0` is a real value."""
FEATURES: Final = (*NUMERIC_FEATURES, *DUMMY_FEATURES)
"""The order that the model expects. The model predicts by position."""
COVERAGE_FEATURES: Final = (*NUMERIC_FEATURES, *DUMMY_GROUPS)
"""The names in the coverage report: each numeric feature, and each group of dummies as one."""

MIN_TRAINING_ROWS: Final = 10_000
MIN_MINORITY_EXAMPLES: Final = 200
MIN_EVENTS: Final = 1_000
MAX_ROW_CHANGE: Final = 0.3
"""How much the rows of a scoring can differ from the last one before the run refuses to publish."""
MAX_P_VALUE: Final = 0.05
MIN_SCORE_DEVIATION: Final = 0.01
"""The least deviation of the raw score, in log-odds. A model with less has collapsed."""

TRAINING_FOLDS: Final = 5
TEST_SIZE: Final = 0.2
BETA: Final = 0.5
SHAP_SAMPLE_SIZE: Final = 2000
SEARCH_KEYS: Final = (
    "num_leaves",
    "max_depth",
    "learning_rate",
    "n_estimators",
    "min_child_samples",
    "subsample",
    "colsample_bytree",
    "reg_alpha",
    "reg_lambda",
)
"""The hyperparameters that the search defines."""
FIXED_PARAMETERS: Final = {"is_unbalance": True, "random_state": RANDOM_STATE, "n_jobs": 1, "verbose": -1}
"""The LightGBM arguments outside the search. `is_unbalance` leaves the probability uncalibrated on purpose."""
PROBABILITY_QUANTILES: Final = (0.01, 0.25, 0.50, 0.75, 0.99)

NEW_MODEL: Final = "modelo_novo"
PRODUCTION_MODEL: Final = "modelo_producao"
HEURISTIC: Final = "heuristica_limiar"
RANDOM_RANKING: Final = "ranking_aleatorio"
SUMMARY_COLUMNS: Final = {
    "pct_evaluated": "% telefone real avaliado",
    "pct_change_failed": "% mudaria | disparo falhou",
    "pct_keep_failed": "% manteria | disparo falhou",
    "pct_change_success": "% mudaria | disparo sucesso",
    "pct_keep_success": "% manteria | disparo sucesso",
    "gap": "gap",
}
"""The header of the summary in the report, by field of `Summary`."""
SUMMARY_METRICS: Final = {
    "pct_evaluated": "pct_avaliado",
    "pct_change_failed": "pct_mudaria_falhou",
    "pct_keep_failed": "pct_manteria_falhou",
    "pct_change_success": "pct_mudaria_sucesso",
    "pct_keep_success": "pct_manteria_sucesso",
    "gap": "gap",
}
"""The name of each field of `Summary` as a metric of the evaluation table."""
BAR_COLOR: Final = "#2a78d6"

MODEL_FILE: Final = "modelo.txt"
METADATA_FILE: Final = "metadata.json"
CHAMPION_FILE: Final = "champion.json"
PREVIOUS_CHAMPION_FILE: Final = "champion_anterior.json"
GCS_PREFIX: Final = "gs://"
TIMEZONE: Final = "America/Sao_Paulo"
