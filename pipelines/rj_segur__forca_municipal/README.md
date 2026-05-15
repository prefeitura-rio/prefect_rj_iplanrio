# rj_segur__forca_municipal

Pipeline de extração de dados da API **HxGN OnCall (CIVITAS/CORIO)** — sistema de gestão de ocorrências e unidades da Guarda Municipal do Rio de Janeiro.

## Visão geral

Extrai dados de 13 endpoints da API, salva em Parquet particionado por data e carrega no BigQuery via GCS. Um único flow parametrizado por `table_id` é executado em schedules independentes para cada tabela.

```
API HxGN OnCall
      │
      ▼
  FMApi (httpx)          autenticação JWT, paginação, retry em 401
      │
      ▼
  save_partitions_task   adiciona updated_at + id_hash, salva Parquet
      │
      ▼
  GCS → BigQuery         dataset brutos_forca_municipal
```

## Tabelas

### Alta frequência

| `table_id` | Endpoint | Frequência | Descrição |
|---|---|---|---|
| `unidades_ativas` | `GET /api/unidades/ativas` | 5 min | Estado atual de todas as unidades ativas |
| `ocorrencias_ativas` | `GET /api/ocorrencias/ativas` | 5 min | Ocorrências em aberto |
| `ocorrencias_ativas_v2` | `GET /api/ocorrencias/ativas/v2` | 5 min | Ocorrências em aberto (v2) |
| `unit_positions` | `GET /api/unit/positions` | 1 min | Posições GPS por unidade ativa (ver abaixo) |

### Frequência diária (02h–03h30 BRT)

| `table_id` | Endpoint | Descrição |
|---|---|---|
| `unidades_historico` | `GET /api/unidades/historico` | Histórico de unidades |
| `ocorrencias_historico` | `GET /api/ocorrencias/historico` | Histórico de ocorrências |
| `qmd` | `GET /api/qmd` | Lista de QMDs (missões diárias) |
| `qmd_servicos` | `GET /api/qmd/servicos` | Serviços associados a QMDs |
| `qmd_missoes` | `GET /api/qmd/missao` | Missões dos QMDs |
| `qmd_plano` | `GET /api/qmd/plano` | Planos dos QMDs |
| `qmd_ativos` | `GET /api/qmd/ativos` | QMDs ativos |
| `qmd_detalhes` | `GET /api/qmd/{id}` | Detalhes completos por QMD (ver abaixo) |
| `qmd_kml` | `GET /api/qmd/{id}/kml` | Geometrias dos QMDs em WKT (ver abaixo) |

### Schemas especiais

#### `unit_positions`

Itera todas as unidades ativas (`/api/unidades/ativas`) e coleta as posições GPS de cada uma via `/api/unit/positions?unitId=...`. Colunas adicionais de partição:

| Coluna | Tipo | Descrição |
|---|---|---|
| *(campos da API)* | string | Campos retornados pela API |
| `id_hash` | string | Hash MD5 do conteúdo (deduplicação no GCS) |
| `updated_at` | datetime | Timestamp da coleta (America/São_Paulo) |

Para backfill, passe `data_inicio`, `data_fim`, `hora_inicio` e `hora_fim` no flow.

#### `qmd_detalhes`

Busca os IDs via `GET /api/qmd` e faz `GET /api/qmd/{id}` para cada um. Campos aninhados (listas e objetos) são serializados como JSON string para compatibilidade com BigQuery.

#### `qmd_kml`

Busca os IDs via `GET /api/qmd` e faz `GET /api/qmd/{id}/kml` para cada um. O KML é parseado com `fastkml` — uma linha por `<Placemark>`, de todos os `<Folder>`:

| Coluna | Tipo | Descrição |
|---|---|---|
| `qmd_id` | string | ID do QMD |
| `kml_folder` | string | Nome do Folder KML (ex: `QMD`, `Missões`) |
| `name` | string | Nome do Placemark |
| `description` | string | HTML do CDATA |
| `geometry_wkt` | string | Geometria em WKT (POINT, LINESTRING, POLYGON…) |
| `geometry_type` | string | Tipo da geometria shapely |
| `extended_data` | string (JSON) | Todos os campos `<ExtendedData>` do Placemark |
| `id_hash` | string | Hash MD5 do conteúdo |
| `updated_at` | datetime | Timestamp da coleta |

Em BigQuery: `ST_GEOGFROMTEXT(geometry_wkt)` converte para o tipo `GEOGRAPHY`.

O schema é estável — novos campos no KML vão para `extended_data` sem quebrar a tabela.

## Parâmetros do flow

| Parâmetro | Tipo | Default | Descrição |
|---|---|---|---|
| `table_id` | string | — | **Obrigatório.** Identificador da tabela (ver lista acima) |
| `dataset_id` | string | `brutos_forca_municipal` | Dataset do BigQuery |
| `dump_mode` | string | `append` | `append` acumula; `overwrite` substitui |
| `biglake_table` | bool | `True` | Cria a tabela como BigLake |
| `page_size` | int | `500` | Registros por página (máximo da API) |
| `data_inicio` | string | hoje | Início do período para `unit_positions` (YYYY-MM-DD) |
| `data_fim` | string | `data_inicio` | Fim do período para `unit_positions` (YYYY-MM-DD) |
| `hora_inicio` | string | `00:00:00` | Hora de início para `unit_positions` |
| `hora_fim` | string | `23:59:59` | Hora de fim para `unit_positions` |

## Variáveis de ambiente

Definidas via secret `prefect-jobs-secrets` no Kubernetes (staging usa o mesmo secret):

| Variável | Descrição |
|---|---|
| `API_FORCA_MUNICIPAL__API_URL` | URL base da API (ex: `https://data.corio-oncall.com.br:8086`) |
| `API_FORCA_MUNICIPAL__API_LOGIN` | Usuário para autenticação (`POST /api/login`) |
| `API_FORCA_MUNICIPAL__API_PASSWORD` | Senha para autenticação |
| `API_FORCA_MUNICIPAL__USE_PROXY_URL` | `true` para rotear via proxy (desenvolvimento local) |
| `API_FORCA_MUNICIPAL__PROXY_URL` | URL do proxy HTTP (usado apenas se `USE_PROXY_URL=true`) |

Para desenvolvimento local, crie o arquivo `.env` na pasta da pipeline:

```env
API_FORCA_MUNICIPAL__API_URL=https://api_url.com:port
API_FORCA_MUNICIPAL__API_LOGIN=usuario
API_FORCA_MUNICIPAL__API_PASSWORD=senha
API_FORCA_MUNICIPAL__USE_PROXY_URL=true
API_FORCA_MUNICIPAL__PROXY_URL=http://proxy:port
```

## Execução local

```bash
# Instalar dependências
uv sync

# Executar um endpoint simples
prefect flow run pipelines/rj_segur__forca_municipal/flow.py:rj_segur__forca_municipal \
  -p table_id=unidades_ativas

# Backfill de posições GPS
prefect flow run pipelines/rj_segur__forca_municipal/flow.py:rj_segur__forca_municipal \
  -p table_id=unit_positions \
  -p data_inicio=2026-05-01 \
  -p data_fim=2026-05-14
```

## Deploy

O deploy é feito via GitHub Actions ao fazer merge na branch principal. Para deploy manual:

```bash
prefect deploy --all --prefect-file pipelines/rj_segur__forca_municipal/prefect.yaml
```

Dois deployments são criados: `staging` (sem schedules) e `prod` (com os 13 schedules).

## Deduplicação

Cada execução calcula um hash MD5 do conteúdo bruto retornado pela API (antes de adicionar `updated_at` e `id_hash`). O hash é usado como sufixo do arquivo Parquet no GCS:

```
data_particao=2026-05-15/data_<hash>.parquet
```

Mesma resposta da API → mesmo hash → mesmo nome de arquivo → overwrite sem duplicata.  
Resposta diferente → novo hash → novo arquivo na partição → histórico de estados preservado.

## Autenticação da API

A API usa JWT. O token é obtido via `POST /api/login` e reutilizado em todas as requisições. Em caso de resposta `401`, o token é renovado automaticamente e a requisição é refeita uma vez.
