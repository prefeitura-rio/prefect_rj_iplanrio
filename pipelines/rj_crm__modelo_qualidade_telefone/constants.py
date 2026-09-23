"""Constantes compartilhadas pelos módulos da pipeline de qualidade de telefone."""

PROJECT_ID = "rj-crm-registry"
DATASET_ID = "crm_whatsapp"
TABLE_ID_SCORE = "telefone_prob_high_delivery"
# Formato longo: data_execucao, versao, origem[treino_holdout|shap|simulacao|gate],
# algoritmo, metrica, valor, detalhe — ver tasks/retreino/publicar.py.
TABLE_ID_AVALIACAO = "telefone_qualidade_modelo_avaliacao"

# Raiz das versões do modelo no GCS — layout e convenções em utils/modelo_store.py.
# Bootstrap já publicado lá (versão 2026-09-21), validado no passo 3 do TODO.
RAIZ_MODELOS = "gs://rj-crm-registry/modelos/qualidade_telefone"

# Pasta "Relatórios de retreino" no Drive (ID dado pelo usuário, 2026-09-23) — precisa
# estar compartilhada (Editor) com a service account de BASEDOSDADOS_CREDENTIALS_<PROD|STAGING>.
# https://drive.google.com/drive/folders/1h4wlF6mq9I7NmVCmR2yn6vycShDKoEpy
DRIVE_PASTA_RAIZ_ID = "1h4wlF6mq9I7NmVCmR2yn6vycShDKoEpy"

# Parâmetros do rótulo HighDelivery e do sorteio de data_corte no treino
# (mesmos valores de qualidade_telefone_modelo/src/qualidade_telefone_modelo/config.py).
RANDOM_STATE = 42
JANELA_FUTURO_DIAS = 30
LIMIAR_HIGH_DELIVERY = 0.90

SISTEMAS = ["agendamento_cadunico", "bcadastro", "cadunico", "ergon", "sms"]

# Parâmetros da heurística de simulação (avaliar_simulacao.py) — mesmos valores de
# qualidade_telefone_modelo/src/qualidade_telefone_modelo/config.py.
LIMIAR_TAXA_ENTREGA = 0.50

# P(telefone HighDelivery | sistema) — de qualidade_telefone_modelo/deliverables/
# ranking_confiabilidade.csv (agregado por sistema, sem CPF/telefone individual). Virou
# constante aqui em vez de CSV versionado: são só 5 números, e um CSV avulso é proibido
# num diretório de pipeline (ver STYLEGUIDE §3.3). Usado no fallback de
# avaliar_simulacao.rankear_heuristica — revisitar se o ranking de confiabilidade mudar.
RANKING_CONFIABILIDADE = {
    "cadunico": 0.928,
    "ergon": 0.920,
    "bcadastro": 0.900,
    "agendamento_cadunico": 0.894,
    "sms": 0.853,
}

FEATURES_NUMERICAS = [
    "qtd_disparo_anterior",
    "taxa_sucesso_anterior",
    "dias_desde_ultimo_disparo",
    "dias_desde_ultimo_disparo_sucesso",
    "dias_desde_ultima_resposta",
    "n_sistemas_registrado",
    "telefone_associado_cnpj",
    *[f"qtd_aparicoes_{s}" for s in SISTEMAS],
    *[f"dias_desde_atualizacao_{s}" for s in SISTEMAS],
    "dias_desde_primeiro_registro",
    "qtd_cpfs_associados",
    "qtd_telefones_cpf_recente",
    "qtd_cpfs_associados_cadunico",
    "qtd_familias_associadas",
    "concentracao_familiar",
    "pct_cpfs_menor_idade",
]

# Dummies calculadas em queries/features_telefone.sql (as categorias fixas moram lá).
FEATURES_DUMMIES = [
    *[f"ddd_{d}" for d in ["11", "21", "22", "24", "83", "outros"]],
    *[f"qualidade_{q}" for q in ["INVALIDO", "SUSPEITO", "VALIDO"]],
]

# Features fracionárias (FLOAT64 na query); todas as outras são contagens/dias/dummies (INT64).
FEATURES_FLOAT = ["taxa_sucesso_anterior", "concentracao_familiar", "pct_cpfs_menor_idade"]

# Ordem que o modelo espera: o LightGBM em formato nativo prediz por posição, então o
# código sempre seleciona `df[FEATURES]` antes de prever, nunca confia na ordem do SQL.
FEATURES = FEATURES_NUMERICAS + FEATURES_DUMMIES
