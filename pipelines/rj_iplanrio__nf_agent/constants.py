"""Constantes do modelo e do Bifrost compartilhadas entre os módulos da pipeline."""

BIFROST_PROVIDER = "vertex"

# Nome sem o prefixo "vertex/": batches.create repassa o valor direto ao Vertex.
MODEL_NAME = "gemini-3.1-flash-lite"

GENERATION_CONFIG: dict[str, float | int | str] = {
    "temperature": 0.1,
    "topP": 0.95,
    "maxOutputTokens": 8192,
    "responseMimeType": "application/json",
}

# O proxy do Bifrost recusa uploads a partir de ~100 MB.
BATCH_MAX_BYTES = 95_000_000
