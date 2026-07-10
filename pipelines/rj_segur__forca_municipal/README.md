# rj_segur__forca_municipal

Pipeline de extração de dados da API **HxGN OnCall** (sistema CIVITAS/CORIO) da **Guarda Municipal do Rio de Janeiro**, com carga para BigQuery via GCS staging.

---

## Visão geral

A API HxGN OnCall expõe dados operacionais da Força Municipal: unidades, ocorrências, posições GPS e Quadros de Missão e Disposição (QMDs). O pipeline extrai esses dados diariamente (e a cada 5 minutos no caso de ocorrências ativas) e os persiste em tabelas de staging no BigQuery.

---

## Estrutura de arquivos

```
rj_segur__forca_municipal/
├── flow.py          # Ponto de entrada: roteamento por table_id para a task correta
├── task.py          # 4 tasks de extração + _process_cycle + _flush_buffer
├── client.py        # FMApi: cliente HTTP com autenticação, pool, retry e paginação
├── bq.py            # Utilitários BigQuery/GCS: query_distinct_ids, delete_gcs_partition
├── utils.py         # Helpers puros: _parse_date_range, _date_range, _iter_placemarks, _cycle_prefix
├── constants.py     # Todas as constantes globais (timeouts, limites, TABLE_CONFIG, etc.)
├── env.py           # Leitura de variáveis de ambiente (com suporte a .env local)
├── prefect.yaml     # Definição de deployments e schedules
├── pyproject.toml   # Dependências do pacote uv
└── Dockerfile       # Imagem Docker para execução em k3s
```

---

## Tabelas

O parâmetro `table_id` do flow determina qual endpoint é consumido e qual task é executada.

| `table_id` | Endpoint | Schedule (BRT) | Tipo | Task |
|---|---|---|---|---|
| `ocorrencias_ativas_v2` | `/api/ocorrencias/ativas/v2` | 00:00 a cada 5 min | série | standard |
| `unidades_historico` | `/api/unidades/historico` | 01:00 diário | snapshot | standard |
| `qmd` | `/api/qmd` | 01:20 diário | snapshot | standard |
| `ocorrencias_historico` | `/api/ocorrencias/historico` | 01:25 diário | snapshot | standard |
| `qmd_servicos` | `/api/qmd/servicos` | 01:45 diário | snapshot | standard |
| `qmd_plano` | `/api/qmd/plano` | 01:50 diário | snapshot | standard |
| `unit_positions` | `/api/unit/positions` | 03:00 diário | snapshot por data GPS | unit_positions |
| `qmd_detalhes` | `/api/qmd/{id}` | 03:20 diário | snapshot | qmd_details |
| `qmd_kml` | `/api/qmd/{id}/kml` | 03:40 diário | snapshot | qmd_kml |
| `qmd_missoes` | `/api/qmd/missao` | — sem schedule ⚠️ | snapshot | standard |
| `unidades_ativas` | `/api/unidades/ativas` | — sem schedule | série | standard |
| `ocorrencias_ativas` | `/api/ocorrencias/ativas` | — sem schedule | série | standard |
| `qmd_ativos` | `/api/qmd/ativos` | — sem schedule | série | standard |

> ⚠️ `qmd_missoes`: endpoint `/api/qmd/missao` **não funciona atualmente**. Removido do schedule do `prefect.yaml`. Disponível no flow para execução manual quando o endpoint for corrigido pela API.
>
> **Sem schedule** (`unidades_ativas`, `ocorrencias_ativas`, `qmd_ativos`, `qmd_missoes`): não há deployment agendado. São disponibilizados no flow para execução manual via Prefect UI ou CLI.

**Snapshot**: na primeira escrita do dia, as partições existentes são deletadas no GCS antes do upload (idempotência por re-run).

**Série** (`ocorrencias_ativas_v2`, `unidades_ativas`, `ocorrencias_ativas`): sem pre-delete. O suffix do arquivo inclui o timestamp do run, acumulando múltiplos snapshots intradiários na mesma partição.

**Dependências de schedule**: `unit_positions` (03:00) aguarda 2 h após `unidades_historico` (01:00). `qmd_detalhes` e `qmd_kml` aguardam 2 h e 2 h20 após `qmd` (01:20), respectivamente.

---

## Parâmetros do flow

```python
rj_segur__forca_municipal(
    table_id: str = "table_id",           # Tabela alvo (obrigatório na prática)
    dataset_id: str = "brutos_forca_municipal",
    dump_mode: str = "append",            # "append" | "overwrite"
    page_size: int = 500,                 # Registros por página (máx. 500)
    concurrency: int = 5,                 # Requisições HTTP simultâneas
    qmd_id_concurrency: int = 5,          # IDs QMD em paralelo (qmd_detalhes/kml)
    unit_id_concurrency: int = 1,         # Unidades em paralelo por dia (unit_positions)
    data_inicio: str | None = None,       # YYYY-MM-DD — apenas unit_positions; default D-1
    data_fim: str | None = None,          # YYYY-MM-DD — apenas unit_positions; default data_inicio
)
```

---

## Arquitetura de processamento

### Buffer/ciclo (`_flush_buffer`)

Todas as tasks usam o helper `_flush_buffer`, que:

1. Consome um `AsyncGenerator` de batches de rows.
2. Acumula rows num buffer interno.
3. Dispara `_process_cycle` a cada `MAX_ROWS_PER_CYCLE` (50 000) rows acumuladas.
4. Faz um flush final com o restante após o último batch.

Isso garante que o uso de memória é limitado independentemente do volume total de dados.

### Ciclo de processamento (`_process_cycle`)

Cada ciclo:

1. Monta um `DataFrame` a partir dos dados recebidos.
2. Calcula `id_hash` (hash de 16 dígitos hex) sobre as colunas originais da API — identificador estável de conteúdo, independente do momento da coleta.
3. Adiciona `updated_at` (timestamp fixo do início do run, idêntico para todos os ciclos).
4. Particiona com `parse_date_columns` e salva em Parquet local.
5. No ciclo 1 de tabelas snapshot: deleta as partições correspondentes no GCS.
6. Faz upload via `create_table_and_upload_to_gcs` (apenas staging, `biglake_table=True`).

### Tasks

**`run_standard_endpoint_task`** — endpoints paginados padrão.
- Busca a página 1 (`get_first`) para obter `total_pages`.
- Calcula `pages_per_cycle = ceil(MAX_ROWS / page_size)` e `total_cycles = ceil(total_pages / pages_per_cycle)`.
- Cada ciclo busca o próximo bloco de páginas em paralelo via `get_pages_range`.

**`run_unit_positions_task`** — posições GPS por unidade, dia a dia.
- Lê os `UnitId`s da última partição de `unidades_historico` no BigQuery.
- Itera sequencialmente por data (`data_inicio` → `data_fim`).
- Dentro de cada dia, busca todas as unidades em lotes de `unit_id_concurrency` com `asyncio.gather(return_exceptions=True)`.
- `partition_col="data_coleta"` (data adicionada pelo pipeline, não pela API).
- Falhas por unidade-dia são contadas; se ultrapassarem `MAX_ITEM_ERROR_RATE` (5%), levanta `RuntimeError`.

**`run_qmd_details_task`** — detalhes por QMD ID.
- Lê os IDs da última partição de `qmd` no BigQuery.
- Busca `/api/qmd/{id}` (objeto único, não paginado) para cada ID em paralelo.
- HTTP 404/410 são ignorados sem contar como erro; outros erros contam para o threshold.
- Campos list/dict são serializados como JSON string.

**`run_qmd_kml_task`** — geometrias KML por QMD ID.
- Lê os IDs da última partição de `qmd` no BigQuery.
- Busca `/api/qmd/{id}/kml` e parseia com `fastkml`.
- Extrai placemarks em qualquer nível de aninhamento (Document → Folder → Placemark).
- IDs sem KML ou sem placemarks contam como erro para o threshold.
- Cada placemark gera uma row com: `qmd_id`, `kml_folder`, `name`, `description`, `geometry_wkt`, `geometry_type`, `extended_data`.

---

## Cliente HTTP (`FMApi`)

- Pool de conexões persistente via `httpx.AsyncClient` (max 20 conexões, 10 keep-alive).
- Autenticação por Bearer token via `POST /api/login`.
- Re-autenticação automática em 401, com lock para evitar thundering herd.
- Retry exponencial com jitter (tenacity) em falhas de rede e HTTP 5xx — até `MAX_RETRIES` (5) tentativas.
- Retry por página (`MAX_PAGE_RETRIES = 2`) em `get_pages_range`: páginas que falham são reagrupadas e re-tentadas em rounds adicionais.
- Suporte a proxy configurável via `API_FORCA_MUNICIPAL__USE_PROXY_URL`.

---

## Variáveis de ambiente

Configuradas via secret Kubernetes (`secretName` no `prefect.yaml`) ou via arquivo `.env` local para desenvolvimento.

| Variável | Descrição |
|---|---|
| `API_FORCA_MUNICIPAL__API_URL` | URL base da API HxGN OnCall |
| `API_FORCA_MUNICIPAL__API_LOGIN` | Usuário para autenticação |
| `API_FORCA_MUNICIPAL__API_PASSWORD` | Senha para autenticação |
| `API_FORCA_MUNICIPAL__USE_PROXY_URL` | `true`/`false` — se deve usar proxy |
| `API_FORCA_MUNICIPAL__PROXY_URL` | URL do proxy (quando `USE_PROXY_URL=true`) |

Para desenvolvimento local, crie `.env` na raiz da pasta da pipeline com essas variáveis.

---

## Deploy

O `prefect.yaml` define dois deployments:

- **`rj_segur--forca_municipal--staging`**: sem schedules, para testes manuais. Usa secret `prefect-jobs-secrets`.
- **`rj_segur--forca_municipal--prod`**: com todos os schedules configurados. Usa secret `prefect-jobs-secrets`.

Ambos rodam no pool `k3s-pool` com a imagem Docker publicada em `ghcr.io/prefeitura-rio/prefect_rj_iplanrio/deployments`.

```sh
# Deploy manual
prefect deploy --all --prefect-file pipelines/rj_segur__forca_municipal/prefect.yaml
```

---

## Tolerância a falhas por item

As tasks `unit_positions`, `qmd_detalhes` e `qmd_kml` operam item a item (unidade/ID). Falhas individuais são capturadas e contadas. Após processar todos os itens, se a taxa de erro superar `MAX_ITEM_ERROR_RATE = 0.05` (5%), o flow falha com `RuntimeError` descrevendo quantos itens falharam.

Para desativar essa tolerância e falhar no primeiro erro, defina `MAX_ITEM_ERROR_RATE = 0.0` em `constants.py`.
