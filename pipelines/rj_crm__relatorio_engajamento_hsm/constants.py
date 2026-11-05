"""Constants of rj_crm__relatorio_engajamento_hsm."""

from typing import Final
from zoneinfo import ZoneInfo

import polars as pl

from utils.types import Cloud, Table

SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
PROJECT_ID: Final = "rj-crm-registry"
DISPATCHES_TABLE: Final = f"{PROJECT_ID}.brutos_salesforce_staging.disparos_ativos"
CHATBOT_DATASET: Final = "rmi_conversas"
CHATBOT_TABLE: Final = "chatbot"
SOURCES: Final = ("HSM", "AI_AGENT_CIDADAO", "AI_AGENT_AGENTE")
"""The sources of the messages of a conversation.

CUSTOMER stays out: AI_AGENT_CIDADAO repeats it with a time.
"""
SOURCE_LABELS: Final = {"HSM": "HSM", "AI_AGENT_CIDADAO": "CIDADÃO", "AI_AGENT_AGENTE": "AGENTE"}
CITIZEN_SOURCE: Final = "AI_AGENT_CIDADAO"
HSM_SOURCE: Final = "HSM"
NO_CATEGORY: Final = "NENHUMA_DO_CATALOGO"
"""What the model answers when no category of the catalog fits.

The conversation then waits for a next discovery."""
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
"""The folder "Relatórios de Engajamento" in Drive, by id and not by name.

No other folder then gets the files. It needs Editor access for the service account."""
DOCX_MIME: Final = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
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
        "3) A planilha '{name}', na mesma pasta deste relatório,",
        "traz o detalhe por conversa usado aqui.",
    ]
)
QUERY_NOTE: Final = " ".join(
    [
        "Consulta em {table} usada pra extrair as conversas deste disparo,",
        "com os parâmetros já substituídos pelos valores reais desta análise.",
    ]
)
FORM_SHEET_ID: Final = "15dmce6ztuj9BW2ds44OZ6kZN3VOAPB3dEZ1UEwd36XY"
FORM_TAB: Final = "Respostas ao formulário 1"
FORM_TIMESTAMP: Final = "Carimbo de data/hora"
FORM_EMAIL: Final = "Endereço de e-mail"
FORM_CAMPAIGN: Final = "Campanha"
FORM_TYPE: Final = "Tipo de solicitação"
FORM_START: Final = "Data de início das conversas"
FORM_END: Final = "Data de fim das conversas"
FORM_GENERATION: Final = "Data de geração"
FORM_STATUS: Final = "status"
FORM_LINK: Final = "link_resultado"
FORM_PROCESSED_AT: Final = "processado_em"
FORM_COLUMNS: Final = (
    FORM_TIMESTAMP,
    FORM_EMAIL,
    FORM_CAMPAIGN,
    FORM_TYPE,
    FORM_START,
    FORM_END,
    FORM_GENERATION,
    FORM_STATUS,
    FORM_LINK,
    FORM_PROCESSED_AT,
)
TYPE_VALIDATION: Final = "Material de validação"
TYPE_REPORT: Final = "Relatório de engajamento"
TYPE_QUICK: Final = "Relatório rápido (catálogo automático)"
REQUEST_TYPES: Final = (TYPE_VALIDATION, TYPE_REPORT, TYPE_QUICK)
STATUS_PROCESSING: Final = "processando"
STATUS_DONE: Final = "concluido"
STATUS_SKIPPED: Final = "pulado"
STATUS_ERROR: Final = "erro"
STATUS_WAITING: Final = "aguardando liberação"
STALE_PROCESSING_HOURS: Final = 6
"""A request in processing for longer than this was killed from outside."""
MAX_REASON_CHARS: Final = 300
DATE_FORMATS: Final = ("%d/%m/%Y", "%Y-%m-%d")
DATETIME_FORMATS: Final = ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S")
PROCESSED_AT_FORMAT: Final = "%d/%m/%Y %H:%M:%S"
DEFAULT_CEILING: Final = 5000
"""The most conversations that one report sends to the model without a release."""
DEFAULT_JUDGE_CEILING: Final = 2000
DEFAULT_CONCURRENCY: Final = 8
DEFAULT_RAG_EXAMPLES: Final = 3
MAX_RAG_CHARS: Final = 1500
MAX_CONVERSATION_TOKENS: Final = 1024
MAX_DISCOVERY_EXAMPLES: Final = 5
CATALOG_FILE: Final = "Catálogo"
EXAMPLES_FILE: Final = "exemplos_nao_classificado"
REPORT_FILE: Final = "relatorio_engajamento"
CONVERSATIONS_FILE: Final = "conversas_classificadas"
AUTO_CATALOG_FILE: Final = "catalogo_automatico"
CATALOG_TAB: Final = "catalogo"
LEGEND_TAB: Final = "legenda"
EXAMPLES_TAB: Final = "exemplos"
CONVERSATIONS_TAB: Final = "conversas"
AUTO_CATALOG_TAB: Final = "catalogo_automatico"
REVIEWED_MARK: Final = "revisada"
QUICK_FOLDER_SUFFIX: Final = "__catalogo_automatico"
"""Files below a folder with this suffix never feed the example store."""
CATALOG_CATEGORY: Final = "Categoria"
CATALOG_DESCRIPTION: Final = "Descrição"
CATALOG_EXAMPLES: Final = "Exemplos de conversas"
CATALOG_DECISION: Final = "Decisão"
CATALOG_REASON: Final = "Justificativa"
CATALOG_DECIDED_ON: Final = "Data da decisão"
CATALOG_COLUMNS: Final = (
    CATALOG_CATEGORY,
    CATALOG_DESCRIPTION,
    CATALOG_EXAMPLES,
    CATALOG_DECISION,
    CATALOG_REASON,
    CATALOG_DECIDED_ON,
)
DECISIONS: Final = ("Keep", "Exclude", "New")
ACTIVE_DECISIONS: Final = ("Keep", "New")
EXCLUDED_DECISION: Final = "Exclude"
DECISION_ALIASES: Final = {
    "keep": "Keep",
    "exclude": "Exclude",
    "new": "New",
    "manter": "Keep",
    "excluir": "Exclude",
    "nova": "New",
}
"""Read the decision in either language, because earlier catalogs use Portuguese."""
CONV_SESSION: Final = "id_sessao"
CONV_CATEGORY: Final = "categoria"
CONV_REASON: Final = "justificativa"
CONV_SUMMARY: Final = "resumo"
CONV_CONVERSATION: Final = "conversa"
CONV_VERDICT: Final = "juiz_veredito"
CONV_EXPECTED: Final = "juiz_categoria_esperada"
CONV_JUDGE_NOTE: Final = "juiz_justificativa"
REVIEW: Final = "revisao"
CORRECT_CATEGORIES: Final = "categorias_corretas"
REVIEW_NOTE: Final = "justificativa_revisao"
REVIEW_RIGHT: Final = "Right"
REVIEW_WRONG: Final = "Wrong"
REVIEW_OPTIONS: Final = (REVIEW_RIGHT, REVIEW_WRONG)
REVIEW_ALIASES: Final = {
    "right": REVIEW_RIGHT,
    "wrong": REVIEW_WRONG,
    "certo": REVIEW_RIGHT,
    "errado": REVIEW_WRONG,
}
"""Read the review in either language, because earlier sheets use Portuguese."""
BOT_RATING: Final = "avaliacao_resposta_bot"
BOT_NOTE: Final = "justificativa_avaliacao_bot"
BOT_OPTIONS: Final = ("Good", "Fair", "Poor")
CONVERSATION_COLUMNS: Final = (
    CONV_SESSION,
    CONV_CATEGORY,
    CONV_REASON,
    CONV_SUMMARY,
    CONV_CONVERSATION,
    CONV_VERDICT,
    CONV_EXPECTED,
    CONV_JUDGE_NOTE,
)
REVIEW_COLUMNS: Final = (REVIEW, CORRECT_CATEGORIES, REVIEW_NOTE, BOT_RATING, BOT_NOTE)
CATEGORY_SEPARATOR: Final = ";"
EXAMPLES_SEPARATOR: Final = "; "
DRIVE_DOC_MIME: Final = "application/vnd.google-apps.document"
DRIVE_SHEET_MIME: Final = "application/vnd.google-apps.spreadsheet"
FLOW_COLOR: Final = (0.267, 0.490, 0.792)
SECRETARIAT_COLOR: Final = (1.0, 0.812, 0.196)
KEEP_COLOR: Final = (0.714, 0.843, 0.659)
EXCLUDE_COLOR: Final = (0.918, 0.600, 0.600)
NEW_COLOR: Final = (1.0, 0.949, 0.667)
EXAMPLES_TABLE: Final = Table(dataset_id="brutos_salesforce", table_id="relatorio_engajamento_exemplos")
"""The Delta table of the examples. The old native table keeps its own name."""
EXAMPLES_CLOUD: Final = Cloud(
    bucket="rj-crm-registry", project="rj-crm-registry", connection="rj-crm-registry.us.biglake"
)
EXAMPLES_SHEET_COLUMNS: Final = {
    "session_id": pl.String,
    "conversation": pl.String,
    "categories": pl.List(pl.String),
    "review": pl.String,
    "review_note": pl.String,
}
"""The examples of one reviewed sheet, before the file and the model are known."""
REVIEW_MARK_DATE: Final = rf"{REVIEWED_MARK}_(\d{{4}}-\d{{2}}-\d{{2}})"
EXAMPLES_SCHEMA: Final = {
    "campaign": pl.String,
    "session_id": pl.String,
    "conversation": pl.String,
    "categories": pl.List(pl.String),
    "review": pl.String,
    "review_note": pl.String,
    "file_id": pl.String,
    "file_name": pl.String,
    "modified_at": pl.String,
    "revision_date": pl.Date,
    "embedding": pl.List(pl.Float64),
    "model": pl.String,
    "ingested_at": pl.Datetime("us", "UTC"),
}
"""The reviewed examples of all campaigns, as the Delta table holds them.

`modified_at` is the Drive time as text, because it is only compared for equality."""
CATALOG_LEGEND: Final = (
    ("Coluna", "O que significa"),
    ("Categoria", "Nome da categoria proposta pela LLM ou criada pela secretaria."),
    ("Descrição", "O que entra nessa categoria."),
    ("Exemplos de conversas", "Lista de id_sessao, separados por ponto e vírgula, de conversas que se encaixam."),
    ("Decisão", "Escolha na lista: Keep, Exclude ou New."),
    ("Justificativa", "Motivo da decisão, em uma frase."),
    ("Data da decisão", "Dia da decisão (AAAA-MM-DD). Serve só para auditoria."),
)
SHEETS_SCOPES: Final = ("https://www.googleapis.com/auth/spreadsheets",)
SHEET_MAX_CELL: Final = 49_000
"""Google Sheets refuses a cell above 50,000 characters, so a longer text is cut."""
SHEET_LAST_ROW: Final = 1000
"""A dropdown or a highlight reaches this row, so added rows have it too."""
SHEET_COLUMN_WIDTH: Final = 220
HEADER_TEXT_COLOR: Final = (1.0, 1.0, 1.0)
DRIVE_FILE_FIELDS: Final = "id, name, mimeType, webViewLink, modifiedTime"
SHEET_ROW: Final = "sheet_row"
"""The column that a read adds: the row number in the sheet, where 1 is the header."""
AUTO_CATALOG_COUNT: Final = "Qtd. de conversas"
CONVERSATIONS_LEGEND: Final = (
    ("Coluna", "O que significa"),
    ("id_sessao", "Identificador único da conversa."),
    ("categoria", "Categoria atribuída pela LLM."),
    ("justificativa", "Motivo da classificação dado pela LLM."),
    ("resumo", "Resumo da conversa gerado pela LLM."),
    ("conversa", "Texto completo da conversa com o cidadão."),
    ("juiz_veredito", "Avaliação do juiz (segunda LLM): CORRETO ou INCORRETO."),
    ("juiz_categoria_esperada", "Categoria que o juiz esperava, quando INCORRETO."),
    ("juiz_justificativa", "Motivo do juiz."),
    ("", ""),
    ("Revisão da classificação", "Preenchida pela secretaria para corrigir a categoria da LLM."),
    ("revisao", "Right: a categoria atribuída está correta. Wrong: está incorreta."),
    ("categorias_corretas", "Quando Wrong, as categorias corretas (separadas por ponto e vírgula)."),
    ("justificativa_revisao", "Motivo da revisão da classificação."),
    ("", ""),
    (
        "Avaliação do bot",
        "Preenchida pela secretaria para avaliar a resposta do bot. Não é usada como feedback do classificador.",
    ),
    (
        "avaliacao_resposta_bot",
        "Good: o bot respondeu bem. Fair: resposta aceitável, com ressalvas. Poor: respondeu mal.",
    ),
    ("justificativa_avaliacao_bot", "Motivo da avaliação do bot."),
)
NO_EXAMPLES: Final = " ".join(
    [
        "(Esta campanha ainda não tem conversas revisadas pela equipe, então não há exemplos.",
        "Classifique só pelo catálogo e pelas regras acima.)",
    ]
)
EXAMPLE_REVIEW_NOTE: Final = "Observação da revisão: {note}"
EXAMPLE_CATEGORIES: Final = "Categorias corretas: {names}"
CLASSIFICATION_TOKENS: Final = 1024
"""The output tokens of the classification of one conversation."""
SUGGESTIONS_NOTE: Final = " ".join(
    [
        "Temas que a LLM viu e o Catálogo não tem. Conversas sem categoria ficam em {none}.",
        'Para usar uma sugestão, inclua a categoria no Catálogo com a decisão "New".',
    ]
)
