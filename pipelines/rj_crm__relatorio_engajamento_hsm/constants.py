"""Constants of rj_crm__relatorio_engajamento_hsm."""

from typing import Final
from zoneinfo import ZoneInfo

SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
PROJECT_ID: Final = "rj-crm-registry"
DISPATCHES_TABLE: Final = f"{PROJECT_ID}.brutos_salesforce_staging.disparos_ativos"
CHATBOT_DATASET: Final = "rmi_conversas"
CHATBOT_TABLE: Final = "chatbot"
SOURCES: Final = ("HSM", "AI_AGENT_CIDADAO", "AI_AGENT_AGENTE")
"""The sources of the messages of a conversation. CUSTOMER stays out: AI_AGENT_CIDADAO repeats it with a time."""
SOURCE_LABELS: Final = {"HSM": "HSM", "AI_AGENT_CIDADAO": "CIDADÃO", "AI_AGENT_AGENTE": "AGENTE"}
CITIZEN_SOURCE: Final = "AI_AGENT_CIDADAO"
HSM_SOURCE: Final = "HSM"
NO_CATEGORY: Final = "NENHUMA_DO_CATALOGO"
"""What the model answers when no category of the catalog fits, and the conversation waits for a next discovery."""
VALID_VERDICTS: Final = frozenset({"CORRETO", "INCORRETO"})
INCORRECT: Final = "INCORRETO"
CORRECT: Final = "CORRETO"
DISCOVERY_SEED: Final = 42
JUDGE_SEED: Final = 42
STALE_BATCHES_LIMIT: Final = 5
"""The batches in a row with no new category that end the discovery."""
DEFAULT_CLASSIFICATION_BATCH: Final = 50
DEFAULT_DISCOVERY_BATCH: Final = 150
DEFAULT_DISCOVERY_CAP: Final = 2000
DEFAULT_JUDGE_ALL_BELOW: Final = 20
DEFAULT_LARGE_CATEGORY_PCT: Final = 30.0
DEFAULT_JUDGE_BATCH: Final = 50
DEFAULT_EXAMPLES: Final = 5
DRIVE_ROOT_ID: Final = "1wpZzSMv6dQ5Y50qWIooqRikpkVCh5u2f"
"""The folder "Relatórios de Engajamento" in Drive, by id and not by name, so no other folder gets the files.

It needs Editor access for the service account."""
DRIVE_FOLDER_MIME: Final = "application/vnd.google-apps.folder"
DOCX_MIME: Final = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
CSV_SEP: Final = ";"
CATEGORY_COLUMNS: Final = (
    "categoria",
    "descricao",
    "qtd",
    "pct_do_engajamento",
    "n_avaliadas_juiz",
    "precisao",
    "margem_erro_wilson_95",
)
DETAIL_COLUMNS: Final = (
    "id_sessao_48h",
    "cpf",
    "telefone",
    "categoria",
    "categoria_justificativa",
    "resumo_gerado",
    "juiz_veredito",
    "juiz_categoria_esperada",
    "juiz_justificativa",
)
Z_95: Final = 1.959963985
"""The critical z of a confidence interval of 95%."""
MAX_EXAMPLE_CHARS: Final = 1200
NOTE_PONTUAL: Final = " ".join(
    [
        "1) Este relatório de engajamento é uma análise pontual das respostas de um disparo,",
        "as categorias criadas aqui não são de monitoramento contínuo.",
    ]
)
NOTE_LLM: Final = " ".join(
    [
        "2) Este relatório foi gerado a partir de uma LLM (um modelo de linguagem) e portanto pode conter",
        "erros. Revise os números com atenção e se atente às métricas e exemplos fornecidos",
        "para não tirar conclusões erradas.",
    ]
)
NOTE_FILES: Final = " ".join(
    [
        "3) Os arquivos {names}, na mesma pasta deste relatório,",
        "trazem o detalhe por categoria e por conversa usados aqui.",
        "Eles usam ponto e vírgula ({separator}) como separador de colunas, não vírgula.",
    ]
)
QUERY_NOTE: Final = " ".join(
    [
        "Consulta em {table} usada pra extrair as conversas deste disparo,",
        "com os parâmetros já substituídos pelos valores reais desta análise.",
    ]
)
