# toolkit

Biblioteca compartilhada das pipelines, com DuckDB como motor: execução de SQL tipada, execução de SQL por templates em qualquer fonte, operações no GCS e no BigQuery, e tasks do Prefect.

As regras do pacote estão em [`STYLEGUIDE.md`](STYLEGUIDE.md).

## Uso

O `toolkit` é um membro do workspace `uv`. Uma pipeline declara a dependência no `pyproject.toml`:

```toml
dependencies = ["toolkit"]

[tool.uv.sources]
toolkit = { workspace = true }
```

```python
from pathlib import Path

from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.executor import Executor
from toolkit.executor.sources.backends.postgres import PostgresSource
from toolkit.executor.sources.protocols import DBConn

source = PostgresSource(DBConn("db.internal", 5432, "app", "reader", "..."))

async with DuckDBEngine.connect() as engine, source.open(engine) as client:
    await source.setup(client)
    executor = Executor(client, Path(__file__).parent / "queries")
    totals = await executor.query("count_events", expect=Total)  # Dataset[Total]
```

## Módulos

| Módulo                     | O que faz                                                                                                                                                                                                                                      |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `toolkit.executor`         | `Executor`: renderiza templates SQL e os roda num cliente (`execute` e `query`); `query` devolve um `Dataset` do Polars checado contra uma classe de schema (`dataset.py` e `schema.py`). O cliente vem do `DuckDBEngine` ou de `Source.open`. |
| `toolkit.settings`         | `Settings` (variáveis de ambiente) e `read_database_credentials`.                                                                                                                                                                              |
| `toolkit.telemetry`        | `logger` (o logger do Prefect, `prefect.toolkit`) e `configure_telemetry()`, que manda logs, traces e métricas a um coletor OpenTelemetry (OTLP/HTTP).                                                                                         |
| `toolkit.gcs`              | `create_store` (um bucket), `list_keys`, `upload` (arquivo ou pasta) e `delete_keys` (obstore, chaves HMAC).                                                                                                                                   |
| `toolkit.bigquery`         | `BigQuery`: `table_exists`, `create_dataset`, `create_external_table`. Para ler dados, use o `BigQuerySource`.                                                                                                                                 |
| `toolkit.executor.sources` | O protocolo `Source` (`open` e `setup`), a base `DuckDBSource` (`duckdb.py`), as fontes fornecidas em `backends/` e `base.py` (`SourceBase`, que registra as subclasses) e `registry.py` (`load_sources()`).                                   |
| `toolkit.dbt`              | Clona o repositório de queries e executa um comando dbt.                                                                                                                                                                                       |

## Executor com uma fonte

```python
async with source.open(engine) as client:  # carrega as extensões e entrega o cliente
    await source.setup(client)  # segredo e ATTACH, por exemplo
    executor = Executor(client, Path(__file__).parent / "queries")
    totals = await executor.query("count_orders", expect=Total)  # Dataset[Total]
    await executor.execute("refresh_view")
```

`Source.open(engine)` entrega o cliente que roda SQL naquele sistema e o fecha ao sair; `Source.setup(client)` prepara o cliente. O `Executor` só tem `execute` e `query`: `query` devolve um resultado tipado (veja "Resultado tipado") e `execute` só roda o statement. O cliente do Postgres, MySQL, SQL Server e BigQuery é o próprio DuckDB, e o SQL do template é DuckDB; o do Oracle é um `OracleClient` sobre `oracledb`, e o SQL do template é Oracle.

## Resultado tipado

```python
class Total(TypedDict):
    nome: str | None  # `| None` aceita nulos; sem isso, nulo é erro
    total: int


totals = await executor.query(
    "count_orders", expect=Total
)  # Dataset[Total]: tudo de uma vez
async for batch in executor.query("orders", expect=Order, batch_size=50_000):
    ...  # um Dataset[Order] por lote
```

O schema é uma classe com anotações (`TypedDict`, dataclass ou `NamedTuple`), sem Pydantic. `Executor.query` devolve um objeto que se pode `await` (um `Dataset` com tudo) ou percorrer com `async for` (um `Dataset` por lote). O cliente devolve quadros do Polars: o DuckDB por `.pl()` (precisa de `pyarrow`, que vem com `polars[pyarrow]`) e o Oracle por `pl.read_database`.

Cada lote é checado, e um `SchemaError` lista todos os problemas, com o nome de cada coluna:

- os nomes das colunas são os do schema, nem mais nem menos; a ordem final é a do schema;
- o tipo tem de ser da mesma família da anotação: qualquer inteiro vira `Int64`, qualquer float vira `Float64`, e decimal e timestamp mantêm a própria precisão; um texto nunca é lido como número;
- uma coluna sem `| None` não pode ter nulos;
- uma coluna `GEOMETRY` do DuckDB (extensão `spatial`) usa a anotação `toolkit.types.Geometry`. O Polars a lê como o tipo de extensão `geoarrow.wkb` (WKB), com o CRS nos metadados do tipo, e a coluna segue assim até o fim, sem conversão;
- uma anotação sem tipo no Polars (por exemplo `set[int]`) levanta `TypeError` antes de rodar a consulta.

Regras de campo (`gt`, `Literal`, regex) não são checadas.

## Engines e fontes

O `Executor` recebe um cliente, e uma fonte é o que abre esse cliente.

- **Engine e cliente:** `DuckDBEngine` é o engine que o pacote fornece. Um cliente é qualquer classe com `execute(sql, params)` e `query(sql, params, batch_size)`, que devolve quadros do Polars (o protocolo `Engine`).
- **Source:** o pacote fornece `PostgresSource`, `MySQLSource`, `MSSQLSource`, `OracleSource`, `BigQuerySource`, `GSheetsSource` e `ArcGISSource` (os três primeiros herdam de `DuckDBSource` e só definem atributos de classe). Uma fonte tem `open(engine)` e `setup(client)`; ela nunca roda uma consulta por conta própria. As classes herdam de `SourceBase` e se registram pelo nome da classe sem `Source` (`PostgresSource` é `postgres`) com a biblioteca `autoregistry`; `load_sources()["postgres"]` acha a classe. Um backend novo é um arquivo novo em `backends/`.

## Configuração

A autenticação do Google é por workload identity (ADC). Não há arquivo de service account. O `ArcGISSource` só carrega a extensão `spatial`; o template lê uma camada com `FROM {{ arcgis_layer(url) }}` (macro de `duckdb/macros.sql`; `url` é a camada sem `/query`, e o segundo argumento é um `where` do ArcGIS) e a geometria mantém o CRS da camada. O `GSheetsSource` pega um token de acesso do ADC com os escopos do Sheets e o entrega à extensão `gsheets` num segredo (o token é um valor ligado, nunca vai no texto do SQL). O template lê com `FROM read_gsheet('<id ou URL>', sheet = 'Aba', range = 'A:F')` e escreve com `COPY tabela TO '<id ou URL>' (FORMAT gsheet)`. O token vale uma hora.

| Variável                                            | Padrão                   | Uso                                                                   |
| --------------------------------------------------- | ------------------------ | --------------------------------------------------------------------- |
| `GCS_HMAC_ACCESS_ID`                                | vazio                    | Chave HMAC do GCS (API compatível com S3).                            |
| `GCS_HMAC_SECRET`                                   | vazio                    | Segredo HMAC do GCS.                                                  |
| `GCS_ENDPOINT`                                      | `storage.googleapis.com` | Endpoint do GCS.                                                      |
| `GCS_REGION`                                        | `auto`                   | Região enviada ao store S3.                                           |
| `GCS_USE_SSL`                                       | `true`                   | Usa HTTPS.                                                            |
| `BIGQUERY_PROJECT`                                  | vazio                    | Projeto das tabelas externas e da fonte `bigquery`.                   |
| `DUCKDB_MEMORY_LIMIT`                               | vazio                    | Limite de memória do DuckDB, por exemplo `4GB`.                       |
| `DUCKDB_TEMP_DIRECTORY`                             | vazio                    | Pasta onde o DuckDB grava o que não cabe na memória.                  |
| `OTEL_EXPORTER_OTLP_ENDPOINT`                       | vazio                    | Coletor OTLP/HTTP, como `http://coletor:4318`. Vazio desliga o envio. |
| `OTEL_EXPORTER_OTLP_HEADERS`                        | vazio                    | Cabeçalhos do coletor, `chave=valor,chave2=valor2`. Segredo.          |
| `OTEL_SERVICE_NAME`                                 | `prefect-rj-iplanrio`    | Nome do serviço nos logs, traces e métricas.                          |
| `LOG_LEVEL`                                         | `INFO`                   | Nível do log.                                                         |
| `<CAMINHO>__DB_USERNAME` e `<CAMINHO>__DB_PASSWORD` | —                        | Credenciais de banco injetadas pelo Infisical.                        |

## Logs, traces e métricas

```python
from toolkit.telemetry import configure_telemetry, logger

configure_telemetry()  # uma vez, depois de importar o Prefect; sem o endpoint não faz nada
logger.info("Files uploaded: prefix=%s files=%d", "ds/tb", 3)
```

A configuração são três settings, lidas do ambiente ou do `.env`: `OTEL_EXPORTER_OTLP_ENDPOINT` (como `http://coletor:4318`) liga o envio, `OTEL_EXPORTER_OTLP_HEADERS` (`chave=valor,chave2=valor2`, um segredo) e `OTEL_SERVICE_NAME` (padrão `prefect-rj-iplanrio`). O toolkit acrescenta `/v1/logs`, `/v1/traces` e `/v1/metrics` ao endpoint.

O destino é o SigNoz, que recebe OTLP/HTTP:

```bash
# SigNoz auto-hospedado: o endpoint HTTP do coletor
OTEL_EXPORTER_OTLP_ENDPOINT=http://signoz-otel-collector:4318

# SigNoz Cloud: a região e a chave de ingestão
OTEL_EXPORTER_OTLP_ENDPOINT=https://ingest.us.signoz.cloud:443
OTEL_EXPORTER_OTLP_HEADERS=signoz-ingestion-key=<chave>
```

A chave de ingestão é um segredo: injete `OTEL_EXPORTER_OTLP_HEADERS` como as outras credenciais, nunca no código nem no `prefect.yaml`. O SigNoz calcula a duração e a taxa de erro de cada operação a partir dos spans, então o toolkit não envia métrica de duração.

**Logs.** O logger é `prefect.toolkit`, então o log tem o formato padrão do Prefect e vai para a UI do Prefect. Com o endpoint, todo log da árvore `prefect` (os do Prefect, os das runs e os do toolkit) segue sozinho para o coletor, e um log dentro de uma run leva o `trace_id` da run. A mensagem é uma frase `<Componente> <ação> <estado>: chave=valor` escrita com `%s`.

**Traces.** O Prefect já cria um span para cada flow run e cada task run (`prefect.run.type`, `prefect.run.id`, tags e os tipos dos parâmetros), com status OK ou ERROR e o mesmo trace para runs aninhadas. O toolkit cria um span para cada operação, filho da run que a chamou:

| Span              | Atributos                                          |
| ----------------- | -------------------------------------------------- |
| `toolkit.query`   | `toolkit.template`, `toolkit.rows`                 |
| `toolkit.execute` | `toolkit.template`                                 |
| `toolkit.upload`  | `toolkit.prefix`, `toolkit.files`, `toolkit.bytes` |
| `toolkit.delete`  | `toolkit.objects`                                  |

Um erro na operação marca o span como ERROR. Parar um resultado antes do fim não é erro: use `contextlib.aclosing`, senão o span só termina quando o gerador é coletado.

**Métricas.** Só há contadores, para os totais que um span não dá:

| Métrica                      | Tipo     | Atributos  |
| ---------------------------- | -------- | ---------- |
| `toolkit.query.rows`         | contador | `template` |
| `toolkit.gcs.uploaded.files` | contador | `prefix`   |
| `toolkit.gcs.uploaded.bytes` | contador | `prefix`   |
| `toolkit.gcs.deleted.files`  | contador | —          |

O `configure_telemetry()` precisa rodar depois de `import prefect`, porque o Prefect refaz a configuração de logging ao ser importado e remove handlers anteriores. Só os loggers `prefect.*` são exportados.

## Desenvolvimento

No diretório `packages/toolkit/` (use `UV_PROJECT_ENVIRONMENT=<repo>/.venv` se o seu shell definir outro ambiente):

```sh
uv run ruff check . && uv run ruff format --check .
uv run basedpyright
uv run pyrefly check
uv run sqlfluff lint toolkit/executor/templates
uv run pytest
```

Veja a Seção 10 do guia para a lista das verificações e a Seção 9 para as regras dos testes. Os testes ficam em `tests/`, com a árvore do pacote, e `pytest -m 'not s3 and not extensions'` roda os que não precisam de serviço nem de rede.

## Mapa de migração

Esta versão removeu a API anterior sem alias. Use a tabela para migrar as pipelines.

| Antes                                                                                   | Agora                                                                                                                                                    |
| --------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `inject_bd_credentials_task`, `get_bd_credentials_from_env`, `get_credentials_from_env` | Removidos. Use ADC: `google.auth.default(scopes=...)`.                                                                                                   |
| `get_database_username_and_password_from_secret_env`                                    | `toolkit.settings.read_database_credentials`                                                                                                             |
| `getenv_or_action`                                                                      | `os.environ` ou `toolkit.settings.Settings`.                                                                                                             |
| `prefect.rename_current_flow_run_task`                                                  | `@flow(flow_run_name="{table_id}")`, do próprio Prefect.                                                                                                 |
| `prefect.generate_dump_db_schedules`, `create_schedules`, `create_dump_db_schedules`    | O bloco `schedules:` escrito no `prefect.yaml` (Seção 8 do guia raiz).                                                                                   |
| `prefect.delete_flow_run_batch`                                                         | Removido.                                                                                                                                                |
| `logging.log`                                                                           | `toolkit.telemetry.logger`.                                                                                                                              |
| `bd.get_storage_blobs`, `gcs.list_blobs_with_prefix`, `gcs.delete_blobs_list`           | `toolkit.gcs.list_keys`, `delete_keys`, `clear_prefix`                                                                                                   |
| `gcs.get_gcs_client`, `gcs.upload_file_to_bucket`                                       | `toolkit.gcs.create_store`, `upload`                                                                                                                     |
| `dbt.execute_dbt_task`                                                                  | `toolkit.dbt.execute_dbt`, dentro de uma task da pipeline                                                                                                |
| `database_sql.Oracle`, `Postgres`, `MySql`, `SqlServer`                                 | `toolkit.executor.sources.registry.load_sources()[nome]`                                                                                                 |
| `pandas.*` (o resto), `io.*`, `constants.NOT_SET`                                       | Removidos até o redesenho do dump.                                                                                                                       |
| `dump_db.tasks.*` (4 tasks)                                                             | Removidos até o redesenho do dump de bancos. Use `Executor.query` e `execute` com um `Source`.                                                           |
| `dump_url.tasks.download_url`, `dump_files`                                             | Para uma URL, aponte o DuckDB para ela (`read_csv('https://…')`) num template. Para um Google Sheet, use o `GSheetsSource`.                              |
| `dump_arcgis.tasks.download_data_from_arcgis_task`                                      | Use o `ArcGISSource` e a macro `arcgis_layer` num template: o DuckDB (`ST_Read`) lê a camada e pagina sozinho. A escrita no bucket depende do redesenho. |
| Os 3 `@flow` do pacote                                                                  | Removidos. O flow fica na pipeline.                                                                                                                      |

Mudanças de comportamento:

- Uma fonte não monta mais consultas: o SQL é um template que o `Executor` roda no cliente da fonte, no dialeto dela. As janelas de data (`window.sql`), `Fetcher` e os passos de limpeza (`Step`, `clean`) foram removidos até o redesenho do dump.
- `charset`, `build_json_dataframe` e `dataframe_key_column` não existem mais.
- O SQL Server usa a extensão `mssql` do DuckDB, e não mais o `pyodbc`.
