# Troubleshooting — rj_iplanrio__nf_agent deploy

## Problema: pipeline não aparece no dashboard do Prefect após CI passar

### #1 — pyproject.toml sem dependências
**Suspeita:** `uv run --package rj_iplanrio__nf_agent -- prefect deploy` falha porque `prefect` e `prefect-docker` não estão nas dependências do pacote.
**Fix:** adicionado `prefect>=3.4.9` e `prefect-docker>=0.6.5` ao `pyproject.toml`.
**Status:** ✅ resolvido — CI passou a entrar no build Docker.

### #2 — CI passava sem deployar a pipeline
**Suspeita:** o script de deploy usa `git diff` para detectar mudanças; sem commits tocando `pipelines/rj_iplanrio__nf_agent/`, a pipeline era pulada e o CI encerrava com código 0.
**Fix:** garantir que cada mudança relevante inclua um arquivo dentro do diretório da pipeline.
**Status:** ✅ confirmado comportamento esperado.

### #3 — Docker build falha em `uv pip install -r requirements.txt`
**Suspeita:** `paddleocr>=2.7.0` exige `paddlepaddle` (não listado, índice customizado); `torch` e `easyocr` também causam falhas. Esses pacotes são lazy imports usados apenas pelo `NFClassifier` (OCR), que esta pipeline não usa.
**Fix:** criado `requirements-prefect.txt` no `agent-nf-validator` sem os pacotes OCR; Dockerfile atualizado para usar esse arquivo.
**Status:** ⚠️ parcialmente resolvido — build ainda falha (ver #4).

### #4 — Docker build falha em `uv pip install -r requirements-prefect.txt`
**Causa raiz confirmada:** `google-generativeai>=0.4.0` requer `protobuf<6.0.0`, mas o workspace tem `override-dependencies = ["grpcio-status==1.78.0"]` que exige `protobuf>=6.31.1`. Conflito irresolvível dentro do mesmo ambiente.
**Fix:** dependências compatíveis movidas para `pyproject.toml` (resolvidas via `uv sync`). `google-generativeai` instalado isoladamente via `pip install --target /opt/agent-packages` no Dockerfile, adicionado ao `PYTHONPATH`.
**Fix definitivo pendente:** migrar `agent-nf-validator` de `google-generativeai` para `google-genai` (novo SDK), que é compatível com protobuf>=6.x. Requer alterar imports em `core/classifiers/gemini_classifier.py` e dependências.
**Status:** ✅ workaround funcional — pipeline apareceu no dashboard e executou.

### #5 — BigQuery NotFound: tabela não encontrada em location US
**Erro:** `NotFound: Table rj-nf-agent:dev_poc_cgm_osinfo.vw_desepesas_recorte_staging was not found in location US`
**Suspeita A:** dataset está em `southamerica-east1`, mas `bigquery.Client` em `bq_input_reader.py` não especifica `location`, usando `US` por padrão.
**Suspeita B:** a view `vw_desepesas_recorte_staging` não existe no dataset de staging.
**Fix (se A):** adicionar `location="southamerica-east1"` ao `bigquery.Client(...)` em `agent-nf-validator/run_poc/bq_input_reader.py`.
**Fix (se B):** criar a view no BigQuery.
**Status:** 🔄 pendente — verificar região e existência do dataset no console GCP.
