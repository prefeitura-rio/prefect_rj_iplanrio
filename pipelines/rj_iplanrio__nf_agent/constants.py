"""Constantes do modelo e do Vertex AI compartilhadas entre os módulos da pipeline."""

# View com um registro por arquivo baixado: nome, último mês de envio e URI do PDF (esteja onde estiver no bucket).
PDFS_VIEW_TABLE = "rj-agent-cgm-triagem-nf.brutos_osinfo_mongo.vw_files_pdfs_download"
VIEW_ORIGIN_PREFIX = "view://"

# Os batches anteriores rodaram em ``global``; o SDK mapeia o nome para publishers/google/models/<nome>.
VERTEX_LOCATION = "global"
MODEL_NAME = "gemini-3.1-flash-lite"

GENERATION_CONFIG: dict[str, float | int | str] = {
    "temperature": 0.1,
    "topP": 0.95,
    "maxOutputTokens": 8192,
    "responseMimeType": "application/json",
}

# O Vertex aceita até 1 GB e 200 mil requisições por job. Ficamos bem abaixo porque o output ecoa
# o request (com a página em base64) e a leitura da saída precisa caber com folga na memória do pod.
BATCH_MAX_BYTES = 250_000_000

# Linhas por sessão. Com a página por URI cada linha é pequena, então o limite de bytes quase nunca aperta;
# este é o que dimensiona a sessão (o Vertex aceita até 200 mil por job).
BATCH_MAX_REQUESTS = 20_000

# Sessões ativas ao mesmo tempo. A submissão para quando chega no teto e deixa o resto para a próxima rodada.
DEFAULT_MAX_ACTIVE_SESSIONS = 20

# Depois da nova tentativa das linhas com erro, se ainda falharem mais que esta fração das páginas, a sessão
# é marcada como falha em vez de gravar resultados: provavelmente é um problema do Vertex, não das páginas,
# e os PDFs voltam a ficar pendentes para uma próxima rodada.
MAX_ERROR_RATIO = 0.5
