# toolkit

Funções e tasks do Prefect reutilizadas pelas pipelines deste monorepo: credenciais, upload para o BigQuery e o GCS, dbt, conectores de banco e templates de extração.

As regras do pacote estão em [`STYLEGUIDE.md`](STYLEGUIDE.md). O layout alvo do pacote é plano (`toolkit.env`, `toolkit.bd`, ...) e está descrito na Seção 3 do guia. Os caminhos de import abaixo são os atuais e vão mudar para ele.

## Uso

O `toolkit` é um membro do workspace `uv`. Uma pipeline declara a dependência no `pyproject.toml`:

```toml
dependencies = ["toolkit"]

[tool.uv.sources]
toolkit = { workspace = true }
```

```python
from toolkit.pipelines_utils.env import inject_bd_credentials_task
from toolkit.pipelines_utils.prefect import rename_current_flow_run_task
```

## Módulos

Os caminhos são relativos a `toolkit.`.

### `pipelines_utils`

| Módulo         | Principais funções                                                                       | Uso                                                                         |
| -------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `env`          | `inject_bd_credentials_task`, `get_bd_credentials_from_env`, `getenv_or_action`          | Credenciais do BigQuery e variáveis de ambiente injetadas pelo Infisical.   |
| `prefect`      | `rename_current_flow_run_task`, `create_schedules`                                       | Renomear a execução do flow e gerar agendamentos.                           |
| `bd`           | `create_table_and_upload_to_gcs_task`, `get_storage_blobs`, `get_project_id`             | Criar tabela no BigQuery e enviar os dados para o GCS.                      |
| `gcs`          | `get_gcs_client`, `upload_file_to_bucket`, `list_blobs_with_prefix`, `delete_blobs_list` | Listar, enviar e apagar objetos no GCS.                                     |
| `dbt`          | `execute_dbt_task`                                                                       | Executar um comando dbt (`run`, `test`, `build`) no repositório de queries. |
| `database_sql` | `Oracle`, `Postgres`, `MySql`, `SqlServer`                                               | Conectores de banco com a mesma interface.                                  |
| `pandas`       | `batch_to_dataframe`, `clean_dataframe`, `to_partitions`, `dataframe_to_parquet`         | Transformações e escrita de DataFrames (será substituído por Polars).       |
| `io`           | `query_to_line`, `is_date`, `human_readable`, `load_yaml_file`                           | Helpers pequenos de texto, datas e arquivos.                                |
| `logging`      | `log`                                                                                    | Escreve no logger da execução do Prefect.                                   |

### `pipelines_templates`

Cada template tem um `tasks.py` com as tasks que as pipelines usam.

| Módulo        | Tasks                                                                                                                                                         | Uso                                                                      |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `dump_db`     | `get_database_username_and_password_from_secret_task`, `format_partitioned_query_task`, `dump_upload_batch_task`, `parse_comma_separated_string_to_list_task` | Extrair dados de um banco para o BigQuery, com partições e lotes.        |
| `dump_url`    | `download_url`, `dump_files`                                                                                                                                  | Baixar arquivos de uma URL, Google Sheets ou Google Drive e carregá-los. |
| `dump_arcgis` | `download_data_from_arcgis_task`                                                                                                                              | Baixar dados do ArcGIS.                                                  |
