# toolkit

Biblioteca compartilhada das pipelines, com DuckDB como motor: execução de SQL tipada, extração de bancos para o GCS, tabelas externas no BigQuery e tasks do Prefect.

As regras do pacote estão em [`STYLEGUIDE.md`](STYLEGUIDE.md).

## Uso

O `toolkit` é um membro do workspace `uv`. Uma pipeline declara a dependência no `pyproject.toml`:

```toml
dependencies = ["toolkit"]

[tool.uv.sources]
toolkit = { workspace = true }
```

```python
from toolkit.dump import DumpRequest
from toolkit.tasks import dump_database_task

dump_database_task(
    DumpRequest(
        database_type="postgres",
        connection={
            "hostname": "db.internal",
            "port": 5432,
            "database": "app",
            "user": "reader",
            "password": "...",
        },
        query="SELECT * FROM events",
        dataset_id="brutos_app",
        table_id="events",
        partition_column="created_at",
    )
)
```

## Módulos

| Módulo                     | O que faz                                                                                                                                                                                 |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `toolkit.executor`         | `Executor`: roda templates SQL num `Engine` (`DuckDBEngine` é o fornecido) lendo de um `Source` que só descreve o sistema remoto; valida as linhas em modelos Pydantic.                   |
| `toolkit.settings`         | `Settings` (variáveis de ambiente) e `read_database_credentials`.                                                                                                                         |
| `toolkit.gcs`              | `create_client`, `list_keys`, `upload_file`, `delete_keys`, `clear_prefix` (aiobotocore, chaves HMAC).                                                                                    |
| `toolkit.bigquery`         | `BigQuery`: `table_exists`, `create_dataset`, `delete_dataset`, `drop_table`, `create_external_table`.                                                                                    |
| `toolkit.executor.sources` | O protocolo `Source`, a base `DuckDBSource` (`duckdb.py`), as fontes fornecidas em `backends/` e `base.py` (`SourceBase`, que registra as subclasses) e `registry.py` (`load_sources()`). |
| `toolkit.partitions`       | Seleções de partição e janelas de data (`plan_selections`, `chunk_windows`).                                                                                                              |
| `toolkit.extraction`       | `COPY` para o bucket de staging, com colunas normalizadas e partições `ano/mes/data_particao`.                                                                                            |
| `toolkit.dump`             | `DumpRequest`, `run_dump` (banco para staging e BigQuery) e `stage_scan` (qualquer `FROM` do DuckDB).                                                                                     |
| `toolkit.dump_url`         | Baixa URL direta, Google Drive ou Google Sheets e lê o CSV.                                                                                                                               |
| `toolkit.dump_arcgis`      | Baixa uma camada ArcGIS e reprojeta a geometria para EPSG:4326 com DuckDB `spatial`.                                                                                                      |
| `toolkit.dbt`              | Clona o repositório de queries e executa um comando dbt.                                                                                                                                  |
| `toolkit.tasks`            | Tasks do Prefect: `dump_database_task`, `dump_url_task`, `dump_arcgis_task`, `execute_dbt_task`, `rename_current_flow_run_task`.                                                          |

## Limpeza

O `Executor` faz toda a limpeza que é SQL no DuckDB. Uma pipeline escreve os arquivos SQL em `queries/` e passa uma lista de passos; o que o DuckDB não faz fica na task da pipeline.

```python
from pathlib import Path

from psycopg.sql import Identifier

from toolkit.dump import run_dump
from toolkit.executor.executor import Step

await run_dump(
    request,
    steps=[Step("trim_names"), Step("drop_duplicates", {"key": Identifier("id")})],
    templates=Path(__file__).parent / "queries",
)
```

Cada passo é um template `SELECT` que lê `{{ source }}`. Os valores são `Identifier`, `Literal`, `SQL`, números ou booleanos; um texto simples levanta `TypeError`, e um erro num passo diz o número e o template. Os passos rodam em ordem, como `TEMP VIEW`s encadeadas, e o resultado é copiado uma vez. Veja a Seção 5.4 do `STYLEGUIDE.md`.

## Engines e fontes

O `Executor` recebe um `Engine` (quem roda o SQL) e um `Source` (de onde vêm os dados).

- **Engine:** `DuckDBEngine` é o que o pacote fornece. Para usar outro, escreva uma classe com `execute(sql, params)` e `records(sql, params)` e passe uma fábrica dela em `run_dump(request, open_engine=...)`.
- **Source:** o pacote fornece `PostgresSource`, `MySQLSource`, `MSSQLSource`, `OracleSource` e `BigQuerySource` (os três primeiros herdam de `DuckDBSource` e só definem atributos de classe). Todas têm `from_context`, que recebe um `SourceContext` (o padrão é `cls()`; só quem precisa do contexto o sobrescreve) (dados de conexão, diretório de trabalho e tamanho de lote) e devolvem um `Source`, que só descreve o sistema remoto: o `Executor` é quem roda o SQL no engine. Para usar outra fonte, escreva uma função (ou classe) que receba um `SourceContext` e devolva algo que siga o protocolo `Source` (`name`, `window`, `extensions`, `setup`, `scan`, e `fetch` se precisar copiar os dados antes) e passe-a em `run_dump(request, make_source=...)`. As classes herdam de `SourceBase` e se registram pelo nome da classe sem `Source` (`PostgresSource` é `postgres`) com a biblioteca `autoregistry`; `load_sources()["postgres"]` acha a classe. Um backend novo é um arquivo novo em `backends/`.

## Configuração

A autenticação do Google é por workload identity (ADC). Não há arquivo de service account.

| Variável                                            | Padrão                   | Uso                                                     |
| --------------------------------------------------- | ------------------------ | ------------------------------------------------------- |
| `GCS_HMAC_ACCESS_ID`                                | vazio                    | Chave HMAC do GCS (API compatível com S3).              |
| `GCS_HMAC_SECRET`                                   | vazio                    | Segredo HMAC do GCS.                                    |
| `GCS_ENDPOINT`                                      | `storage.googleapis.com` | Endpoint do GCS.                                        |
| `GCS_REGION`                                        | `auto`                   | Região enviada ao cliente S3.                           |
| `GCS_USE_SSL`                                       | `true`                   | Usa HTTPS.                                              |
| `STAGING_BUCKET`                                    | vazio                    | Bucket de staging.                                      |
| `BIGQUERY_PROJECT`                                  | vazio                    | Projeto das tabelas externas e da fonte `bigquery`.     |
| `BIGQUERY_BIGLAKE_CONNECTION`                       | vazio                    | Conexão BigLake. Vazio cria uma tabela externa simples. |
| `DUMP_CONCURRENCY`                                  | `1`                      | Seleções extraídas em paralelo.                         |
| `DUMP_RETRY_ATTEMPTS`                               | `3`                      | Tentativas por seleção.                                 |
| `DUMP_RETRY_DELAY_SECONDS`                          | `300`                    | Espera entre tentativas.                                |
| `DUCKDB_MEMORY_LIMIT`                               | vazio                    | Limite de memória do DuckDB, por exemplo `4GB`.         |
| `DUCKDB_TEMP_DIRECTORY`                             | vazio                    | Pasta onde o DuckDB grava o que não cabe na memória.    |
| `LOG_LEVEL`                                         | `INFO`                   | Nível do log.                                           |
| `<CAMINHO>__DB_USERNAME` e `<CAMINHO>__DB_PASSWORD` | —                        | Credenciais de banco injetadas pelo Infisical.          |

## Desenvolvimento

No diretório `packages/toolkit/` (use `UV_PROJECT_ENVIRONMENT=<repo>/.venv` se o seu shell definir outro ambiente):

```sh
uv run ruff check . && uv run ruff format --check .
uv run basedpyright
uv run sqlfluff lint toolkit/templates
uv run pytest
```

Veja a Seção 10 do guia para a lista das verificações.

## Mapa de migração

Esta versão removeu a API anterior sem alias. Use a tabela para migrar as pipelines.

| Antes                                                                                   | Agora                                                                                  |
| --------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `inject_bd_credentials_task`, `get_bd_credentials_from_env`, `get_credentials_from_env` | Removidos. Use ADC: `google.auth.default(scopes=...)`.                                 |
| `get_database_username_and_password_from_secret_env`                                    | `toolkit.settings.read_database_credentials`                                           |
| `getenv_or_action`                                                                      | `os.environ` ou `toolkit.settings.Settings`.                                           |
| `prefect.rename_current_flow_run_task`                                                  | `toolkit.tasks.rename_current_flow_run_task`                                           |
| `prefect.generate_dump_db_schedules`, `create_schedules`, `delete_flow_run_batch`       | Removidos. Os agendamentos ficam no `prefect.yaml`.                                    |
| `logging.log`                                                                           | `toolkit.log.logger`, ou `get_run_logger()` dentro de uma task.                        |
| `bd.create_table_and_upload_to_gcs_task`                                                | `toolkit.dump.stage_scan` (ou as tasks de dump).                                       |
| `bd.get_storage_blobs`, `gcs.list_blobs_with_prefix`, `gcs.delete_blobs_list`           | `toolkit.gcs.list_keys`, `delete_keys`, `clear_prefix`                                 |
| `gcs.get_gcs_client`, `gcs.upload_file_to_bucket`                                       | `toolkit.gcs.create_client`, `upload_file`                                             |
| `dbt.execute_dbt_task`                                                                  | `toolkit.tasks.execute_dbt_task`                                                       |
| `database_sql.Oracle`, `Postgres`, `MySql`, `SqlServer`                                 | `toolkit.executor.sources.registry.load_sources()[nome]`                               |
| `pandas.*`, `io.*`, `constants.NOT_SET`                                                 | Removidos. O DuckDB faz a limpeza e a normalização; `toolkit.columns` normaliza nomes. |
| `dump_db.tasks.*` (4 tasks)                                                             | `toolkit.tasks.dump_database_task` com um `DumpRequest`.                               |
| `dump_url.tasks.download_url`, `dump_files`                                             | `toolkit.tasks.dump_url_task`                                                          |
| `dump_arcgis.tasks.download_data_from_arcgis_task`                                      | `toolkit.tasks.dump_arcgis_task`                                                       |
| Os 3 `@flow` do pacote                                                                  | Removidos. O flow fica na pipeline.                                                    |

Mudanças de comportamento:

- Os filtros de data usam o dialeto de cada banco e a coluna precisa ser um identificador simples.
- As janelas `bimester`, `trimester`, `quadrimester` e `semester` não se sobrepõem mais.
- Sem tabela no BigQuery, a execução é uma carga completa.
- `charset`, `build_json_dataframe` e `dataframe_key_column` não existem mais.
- O SQL Server usa a extensão `mssql` do DuckDB, e não mais o `pyodbc`.
