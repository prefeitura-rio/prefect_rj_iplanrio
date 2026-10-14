# `toolkit`

Biblioteca de uso comum, com `DuckDB` como motor: SQL tipado em qualquer fonte, `GCS`, BigQuery, `dbt` e telemetria. As regras do pacote estão em [`STYLEGUIDE.md`](STYLEGUIDE.md). O comportamento de cada função está na `docstring` dela.

## Uso

O `toolkit` é um membro do `workspace` `uv`. Cada pipeline declara a dependência no `pyproject.toml`:

```toml
dependencies = ["toolkit"]

[tool.uv.sources]
toolkit = { workspace = true }
```

```python
from pathlib import Path
from typing import TypedDict

from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.executor import Executor
from toolkit.executor.sources.backends.postgres import PostgresSource
from toolkit.executor.sources.protocols import DBConn


class Total(TypedDict):
    name: str | None  # `| None` aceita nulos
    total: int


source = PostgresSource(DBConn("db.internal", 5432, "app", "reader", "..."))

async with DuckDBEngine.connect() as engine, source.open(engine) as client:
    await source.setup(client)
    executor = Executor(client, Path(__file__).parent / "queries")
    totals = await executor.query("count_events", expect=Total)  # Dataset[Total]
```

O SQL é um modelo `Jinja` em `queries/count_events.sql`, que lê o banco anexado como `src`.

## Módulos

| Módulo              | O que faz                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `toolkit.executor`  | `Executor`, `Engine`, fontes e modelos SQL. `query` devolve um `Dataset` tipado.                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `toolkit.settings`  | `Settings` (variáveis de ambiente) e `read_database_credentials`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `toolkit.functions` | As funções de toda conexão `DuckDB`, que se registram sozinhas com `@native` e `@arrow`. `native.py` (uma vez por linha): `normalize(coluna, separador, minúsculas)`, `now_sao_paulo` e `to_sao_paulo`. `arrow.py` (uma vez por lote): `clean_text`, `parse_date`, `parse_timestamp`, `ano_particao`, `mes_particao` e `data_particao`. `documents.py` (uma vez por lote): `is_cpf`, `normalize_cpf`, `is_cnpj` (com as letras do CNPJ novo), `normalize_cnpj` e `is_phone` (telefone fixo ou celular do Brasil, com `55` ou sem). |
| `toolkit.utils`     | `normalize_column`, `column_aliases`, `select_columns` (a lista de colunas de um `dump`: `select`, nomes, `clean_text` e `transforms`) e `filter_predicates` (os `filters`, com `Identifier`, `Literal` e a lista fixa de operadores). `Executor.normalize_columns(tabela, colunas, separador, minúsculas)` renomeia colunas.                                                                                                                                                                                                      |
| `toolkit.telemetry` | O `logger` do pacote e `configure_telemetry()`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `toolkit.batches`   | Janelas de um `dump` (`windows`, `pending`, `window_predicates`) e o `hash` da `config` (`config_hash`, `ledger_id`).                                                                                                                                                                                                                                                                                                                                                                                                              |
| `toolkit.delta`     | A tabela `Delta` de um `dump`: `fingerprint`, `known_fingerprints`, `write_window`, `sweep` e `maintain`. O `deltalake` lê o ADC pelo `object_store`: servidor de metadados do GKE, service account ou usuário, e não `external_account`.                                                                                                                                                                                                                                                                                          |
| `toolkit.gcs`       | `create_store`, `list_keys`, `upload` e `delete_keys` (`obstore`, chaves `HMAC`).                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `toolkit.bigquery`  | `BigQuery`: `table_exists`, `create_dataset` e `sync_delta_table` (tabela `Delta` por uma conexão `BigLake`).                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `toolkit.oracle`    | `Oracle`, um `Engine`: `connect`, `execute` e `query`. A consulta volta em `Polars`, nos tipos que o `Delta` guarda.                                                                                                                                                                                                                                                                                                                                                                                                               |
| `toolkit.dbt`       | Clona o repositório de consultas e executa um comando `dbt`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

Fontes (`toolkit.executor.sources.backends`): `PostgresSource`, `MySQLSource`, `MSSQLSource`, `OracleSource` (entrega o `Engine` `Oracle`; cada janela vira `Parquet` local), `BigQuerySource`, `GSheetsSource` e `ArcGISSource`. `load_sources()["postgres"]` acha a classe pelo nome.

## Configuração

A autenticação do Google é por workload identity (ADC), sem arquivo de service account. As variáveis são lidas do ambiente ou do `.env`:

| Variável                                            | Padrão                   | Uso                                                                                                            |
| --------------------------------------------------- | ------------------------ | -------------------------------------------------------------------------------------------------------------- |
| `GCS_HMAC_ACCESS_ID`                                | vazio                    | Chave `HMAC` do `GCS` (API compatível com S3).                                                                 |
| `GCS_HMAC_SECRET`                                   | vazio                    | Segredo `HMAC` do `GCS`.                                                                                       |
| `GCS_ENDPOINT`                                      | `storage.googleapis.com` | Endpoint do `GCS`.                                                                                             |
| `GCS_REGION`                                        | `auto`                   | Região enviada ao `store` S3.                                                                                  |
| `GCS_USE_SSL`                                       | `true`                   | Usa HTTPS.                                                                                                     |
| `BIGQUERY_PROJECT`                                  | vazio                    | Projeto das tabelas externas e da fonte `bigquery`.                                                            |
| `DB_QUERY_TIMEOUT_SECONDS`                          | `3600`                   | Limite de uma consulta no SQL Server (o padrão da extensão é 30 s).                                            |
| `ORACLE_CLIENT_LIB_DIR`                             | vazio                    | Pasta do `Instant Client` do Oracle. Vazio usa o `ldconfig`, e sem o cliente o `oracledb` roda em modo `thin`. |
| `DUCKDB_MEMORY_LIMIT`                               | vazio                    | Limite de memória do `DuckDB`, como `4GB`.                                                                     |
| `DUCKDB_TEMP_DIRECTORY`                             | vazio                    | Pasta onde o `DuckDB` grava o que não cabe na memória.                                                         |
| `OTEL_EXPORTER_OTLP_ENDPOINT`                       | vazio                    | Coletor `OTLP/HTTP`, como `http://coletor:4318`. Vazio desliga o envio.                                        |
| `OTEL_EXPORTER_OTLP_HEADERS`                        | vazio                    | Cabeçalhos do coletor, `chave=valor,chave2=valor2`. Segredo.                                                   |
| `OTEL_SERVICE_NAME`                                 | `prefect-rj-iplanrio`    | Nome do serviço nos logs, traces e métricas.                                                                   |
| `LOG_LEVEL`                                         | `INFO`                   | Nível do log.                                                                                                  |
| `<CAMINHO>__DB_USERNAME` e `<CAMINHO>__DB_PASSWORD` | —                        | Credenciais de banco injetadas pelo `Infisical`.                                                               |

## Logs, traces e métricas

```python
from toolkit.telemetry import configure_telemetry, logger

configure_telemetry()  # uma vez, depois de `import prefect`
logger.info("Files uploaded: prefix=%s files=%d", "ds/tb", 3)
```

O destino é o `SigNoz`, por `OTLP/HTTP`. O `toolkit` acrescenta `/v1/logs`, `/v1/traces` e `/v1/metrics` ao endpoint:

```bash
# SigNoz auto-hospedado
OTEL_EXPORTER_OTLP_ENDPOINT=http://signoz-otel-collector:4318

# SigNoz Cloud
OTEL_EXPORTER_OTLP_ENDPOINT=https://ingest.us.signoz.cloud:443
OTEL_EXPORTER_OTLP_HEADERS=signoz-ingestion-key=<chave>
```

A chave de ingestão é um segredo: injete `OTEL_EXPORTER_OTLP_HEADERS` como as outras credenciais, nunca no código nem no `prefect.yaml`.

- **Logs:** o `logger` é `prefect.toolkit`, com o formato padrão do `Prefect`. Com o endpoint, todo log da árvore `prefect` segue para o coletor, e um log numa `run` leva o `trace_id` dela.
- **Traces:** o `Prefect` cria um `span` por `flow run` e por `task run`. O `toolkit` cria um por operação, filho da `run`: `toolkit.query`, `toolkit.execute`, `toolkit.upload` e `toolkit.delete`. O `SigNoz` calcula duração e taxa de erro a partir deles.
- **Métricas:** só contadores, para os totais que um `span` não dá: `toolkit.query.rows`, `toolkit.gcs.uploaded.files`, `toolkit.gcs.uploaded.bytes` e `toolkit.gcs.deleted.files`.
- Os `flows` de `prefect_rj_iplanrio.flows` já chamam `configure_telemetry()` ao serem importados. `configure_telemetry()` roda depois de `import prefect`, porque o `Prefect` refaz o `logging` ao ser importado e remove os `handlers` anteriores.
- Pare um resultado antes do fim com `contextlib.aclosing`, senão o `span` só termina quando o gerador é coletado.

## Desenvolvimento

No diretório `packages/toolkit/` (use `UV_PROJECT_ENVIRONMENT=<repo>/.venv` se o seu `shell` definir outro ambiente):

```sh
uv run ruff check . && uv run ruff format --check .
uv run basedpyright
uv run pyrefly check
uv run sqlfluff lint toolkit/executor/templates
uv run vulture toolkit
uv run pytest
```

`pytest -m 'not s3 and not extensions'` roda só os testes que não precisam de serviço nem de rede. As regras dos testes estão na Seção 7 do guia, e a lista das verificações na Seção 8.
