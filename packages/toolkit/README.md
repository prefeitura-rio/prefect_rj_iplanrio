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

| Módulo              | O que faz                                                                                                                                               |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `toolkit.executor`  | `Executor`, `Engine`, as fontes e os modelos SQL. `query` devolve um `Dataset` tipado.                                                                  |
| `toolkit.functions` | As funções do `DuckDB`, que toda conexão tem. Veja a lista abaixo.                                                                                      |
| `toolkit.batches`   | As janelas de um `dump` e o `hash` da configuração.                                                                                                     |
| `toolkit.delta`     | A tabela `Delta` de um `dump`: `write_window` (troca uma janela, soma linhas com `append` ou grava a hora com `extracted_at`), `compact` e `maintain`.  |
| `toolkit.gcs`       | `create_store`, `list_keys`, `upload` e `delete_keys`, com `obstore` e chaves `HMAC`.                                                                   |
| `toolkit.bigquery`  | `BigQuery`: `table_exists`, `create_dataset`, `execute` (uma instrução, como um `MERGE`) e `sync_delta_table` (tabela `Delta` por uma conexão BigLake). |
| `toolkit.oracle`    | `Oracle`, um `Engine` com `connect`, `execute` e `query`. A consulta volta em `Polars`, nos tipos que o `Delta` guarda.                                 |
| `toolkit.dbt`       | Clona o repositório de consultas e roda um comando `dbt`.                                                                                               |
| `toolkit.settings`  | `Settings` (variáveis de ambiente), `read_secret` (as variáveis de um caminho de segredo) e `read_database_credentials`.                                |
| `toolkit.utils`     | As colunas e os filtros de um `dump`: `normalize_column`, `column_aliases`, `select_columns` e `filter_predicates`.                                     |
| `toolkit.telemetry` | O `logger` do pacote e `configure_telemetry()`.                                                                                                         |

**Funções do `DuckDB`** (em SQL, sem importar nada):

- Texto: `normalize(coluna, separador, minúsculas)` e `clean_text`.
- Datas: `parse_date`, `parse_timestamp`, `ano_particao`, `mes_particao`, `data_particao`, `now_sao_paulo` e `to_sao_paulo`.
- Documentos: `is_cpf`, `normalize_cpf`, `is_cnpj` (aceita as letras do CNPJ novo), `normalize_cnpj` e `is_phone` (telefone fixo ou celular do Brasil, com ou sem `55`).
- Nomes de colunas: `Executor.normalize_columns(tabela, colunas, separador, minúsculas)`.

**Fontes** (`toolkit.executor.sources.backends`): `PostgresSource`, `MySQLSource`, `MSSQLSource`, `OracleSource`, `CouchDBSource`, `BigQuerySource`, `GSheetsSource` e `ArcGISSource`. `load_sources()["postgres"]` acha a classe pelo nome.

## Bancos: tipos e limites

Conferido em Postgres, MySQL, SQL Server e Oracle de verdade.

**Tipos**

- O `Delta` não guarda `TIME`, `UUID` nem `INTERVAL`: eles chegam como texto. Um inteiro sem sinal chega no inteiro com sinal maior (`UINTEGER` em `BIGINT`), e `UBIGINT` em `DECIMAL(20, 0)`.
- Um `timestamp` com fuso (`timestamptz`, `datetimeoffset`) é cortado em dias de São Paulo, em qualquer máquina. Um `timestamp` sem fuso vale como vem do banco. O `TIMESTAMP` do MySQL vem em UTC.
- `normalize_values` limpa o texto de todo banco, inclusive o `nvarchar` do SQL Server.

**SQL Server**

- O `DuckDB` corta toda consulta em 30 s. O toolkit troca esse limite por `DB_QUERY_TIMEOUT_SECONDS` (3600).
- Numa consulta nativa, uma coluna `varchar` com acento falha com `Invalid unicode`. Use `CAST(coluna AS NVARCHAR(n))` ou leia pela tabela (`read.table`).

**Oracle**

- O `oracledb` lê com o `Instant Client` (modo `thick`, que a imagem base já tem). Sem ele, o `oracledb` usa o modo `thin`, que não lê usuários com verifier `10G`. A pasta do cliente vem de `ORACLE_CLIENT_LIB_DIR` ou do `ldconfig`.
- Cada janela vira um `Parquet` local, com filtros e janela aplicados pelo Oracle. `read.table` é `DONO.TABELA`, `read.query` é uma consulta (pode ter `;` no fim), e as duas aceitam `batches` e `filters`. Os nomes das colunas são os do Oracle, em maiúsculas: `batches: { column: DATA_PEDIDO }`.
- A tabela é lida `AS OF SCN`, com o mesmo `SCN` em todas as janelas, e uma mudança feita durante a carga não entra. Isso pede `EXECUTE` em `SYS.DBMS_FLASHBACK` ou `SELECT` em `SYS.V_$DATABASE`, e `FLASHBACK` na tabela de outro dono. Sem esses privilégios a carga segue sem o `SCN` e avisa no log. Uma consulta nunca é lida `AS OF SCN`.
- Um `NUMBER` nunca perde dígitos: até 18 dígitos sem escala é `int64`, com precisão e escala que cabem é `decimal128`, `FLOAT` é `double`, e um `NUMBER` sem precisão chega como texto exato.
- `DATE` e `TIMESTAMP` chegam sem fuso. `TIMESTAMP WITH TIME ZONE` e `WITH LOCAL TIME ZONE` chegam como o instante em UTC. `CLOB`, `NCLOB`, `NVARCHAR2`, `XMLTYPE`, `INTERVAL` e `ROWID` chegam como texto, e `RAW` e `BLOB` como bytes.
- Um tipo sem conversão (objeto, `BFILE`, `BOOLEAN`, `JSON`) falha e diz a coluna: converta na consulta.

**Postgres e MySQL:** se o servidor parar no meio de uma leitura, a execução espera para sempre. Ponha `timeout_seconds` na tarefa.

**ArcGIS:** `crs`, como `EPSG:31983`, faz o servidor responder nesse sistema de coordenadas. A geometria vira uma coluna `geoarrow.wkb` com o sistema gravado.

**Google Sheets:** o `DuckDB` lê todo número da planilha como `DOUBLE`. A planilha precisa estar compartilhada com a service account.

**CouchDB**

- O `CouchDBSource` lê `conn.database` pela API HTTP e grava `Parquet`, como o Oracle. Há três leituras: `_all_docs` (padrão), `_find` (`query`, um seletor Mango em JSON) e `_changes` (`since`: `0`, `now` ou o `last_seq` anterior).
- As colunas vêm de um `Shape` (`columns` e `row(item)`). O padrão grava `id` e `doc`, que o `DuckDB` lê como `JSON`: `doc->>'$.campo'`. O `BigQuery` lê `doc` como `STRING`: use `PARSE_JSON`.
- Uma janela corta os ids numéricos com `startkey` e `endkey`. Com `key_width=N`, o corte usa os N primeiros dígitos e serve a ids de larguras mistas. A janela `null` lê o resto, como os ids com letra. Os `_design/` nunca entram.
- Sem `filters`. `query` e `since` não vão juntos, e cada um é uma janela só.
- `page_size`, `verify_tls`, `secure`, `path_prefix` e `timeout` vêm do construtor ou das variáveis `COUCHDB_*` da tabela abaixo. `key_width` só vem do construtor. `_all_docs` rendeu cerca de 30 000 documentos por segundo, e `_changes` 13 000: use `_changes` só para cargas incrementais.

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
| `COUCHDB_PAGE_SIZE`                                 | `10000`                  | Documentos por página de uma leitura do CouchDB.                                                               |
| `COUCHDB_PATH_PREFIX`                               | vazio                    | Prefixo do caminho de um CouchDB atrás de um proxy, como `/couchdb`.                                           |
| `COUCHDB_SECURE`                                    | vazio                    | `true` usa HTTPS e `false` usa HTTP. Vazio usa HTTP só na porta 5984.                                          |
| `COUCHDB_TIMEOUT_SECONDS`                           | `300`                    | Segundos que uma página de uma leitura do CouchDB pode levar.                                                  |
| `COUCHDB_VERIFY_TLS`                                | `true`                   | Verifica o certificado do CouchDB. `false` aceita um certificado que não casa com o endereço.                  |
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
- Os `flows` de `utils.flows` já chamam `configure_telemetry()` ao serem importados. `configure_telemetry()` roda depois de `import prefect`, porque o `Prefect` refaz o `logging` ao ser importado e remove os `handlers` anteriores.
- Pare um resultado antes do fim com `contextlib.aclosing`, senão o `span` só termina quando o gerador é coletado.

## Desenvolvimento

Na raiz do repositório, o `devenv` roda as verificações (veja o [README da raiz](../../README.md#desenvolvimento)):

```sh
devenv --profile checks tasks run lint:all test:all
```

Ou, no diretório `packages/toolkit/`, uma ferramenta de cada vez:

```sh
uv run ruff check . && uv run ruff format --check .
uv run basedpyright
uv run pyrefly check
uv run sqlfluff lint toolkit/executor/templates
uv run vulture toolkit
uv run pytest
```

`pytest -m 'not s3 and not extensions'` roda só os testes que não precisam de serviço nem de rede. As regras dos testes estão na Seção 7 do guia, e a lista das verificações na Seção 8.
