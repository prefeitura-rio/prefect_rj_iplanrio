"""Constantes do modelo e do Vertex AI compartilhadas entre os módulos da pipeline."""

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
