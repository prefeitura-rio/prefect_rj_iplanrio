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

### #7 — KeyError: 'descricao_limpa'
**Erro:** `KeyError('descricao_limpa')` em `processor.py` linha 1624.
**Causa raiz:** `vw_despesas_recorte_teste` vem de `rj-cvl.adm_contrato_gestao.despesas` via `SELECT d.*`, que expõe `descricao` mas não `descricao_limpa`. A tabela original `osinfo_despesas_recorte` tinha `descricao_limpa` pré-computada via `REGEXP_REPLACE(descricao, r'(?i)\.pdf$', '')`.
**Fix:** `bq_input_reader.py` deriva `descricao_limpa` de `descricao` após fetch quando a coluna não existe no dataframe.
**Status:** ✅ corrigido em `agent-nf-validator/run_poc/bq_input_reader.py`.

### #6 — BadRequest: type mismatch no JOIN de id_documento (STRING vs INT64)
**Erro:** `No matching signature for operator = for argument types: STRING, INT64 at [5:18]`
**Causa raiz:** `vw_despesas_recorte_teste` expõe `id_documento` como `STRING`, mas `controle_processamento_staging` tem `id_documento` como `INTEGER`. O JOIN em `bq_input_reader.py` usava `=` direto sem cast.
**Fix:** substituído `v.id_documento = c.id_documento` por `CAST(v.id_documento AS STRING) = CAST(c.id_documento AS STRING)` em `read_unprocessed_batch` e `count_pending`.
**Status:** ✅ corrigido em `agent-nf-validator/run_poc/bq_input_reader.py`.

### #8 — FileNotFoundError: gcs-pdf-list.csv não existe no container
**Erro:** `FileNotFoundError: /opt/agent-nf-validator/run_poc/gcs-pdf-list.csv`
**Causa raiz:** `processor.py` chama `get_available_pdf_filenames_from_csv()` que espera um CSV pré-gerado localmente (artefato de dev para evitar chamadas à API GCS). No container esse arquivo não existe.
**Fix:** `gcs_downloader.py` agora faz fallback para `get_available_pdf_filenames()` (listagem via GCS API) quando o CSV não é encontrado.
**Status:** ✅ corrigido em `agent-nf-validator/run_poc/gcs_downloader.py`.

### #5 — BigQuery NotFound: tabela não encontrada em location US
**Erro:** `NotFound: Table rj-nf-agent:dev_poc_cgm_osinfo.vw_desepesas_recorte_staging was not found in location US`
**Suspeita A:** dataset está em `southamerica-east1`, mas `bigquery.Client` em `bq_input_reader.py` não especifica `location`, usando `US` por padrão.
**Suspeita B:** a view `vw_desepesas_recorte_staging` não existe no dataset de staging.
**Fix (se A):** adicionar `location="southamerica-east1"` ao `bigquery.Client(...)` em `agent-nf-validator/run_poc/bq_input_reader.py`.
**Fix (se B):** criar a view no BigQuery.
**Status:** 🔄 pendente — verificar região e existência do dataset no console GCP.

