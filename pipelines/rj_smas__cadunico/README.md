# rj_smas__cadunico

Carrega os arquivos do CadÚnico, gera os modelos `dbt` de cada tipo de registro e os materializa no BigQuery.

## Fluxo

1. Cada ZIP de `raw.records_prefix` cuja data não está na tabela `tables.records` vira um dia dela. A data vem do nome do arquivo e a carga é um `append` com essa data como captura, então uma nova execução não duplica nada.
2. Com ZIP novo, ou com `force_create_models`, as planilhas de layout de `raw.layout_prefix` carregam na tabela `tables.layout` se o bucket tem uma versão que a tabela não tem. A tabela é sempre reescrita por inteiro.
3. Com layout novo, ou com `force_create_models`, o flow lê o layout junto do dicionário de colunas (`layout_dicionario_colunas`), grava o controle de colunas entre versões em `<layout>_columns_version_control`, escreve um modelo `dbt` por tipo de registro e faz `push` na branch de `dbt`.
4. O flow roda o comando `dbt` de `dbt` nessa branch. Sem ZIP novo e sem `force_create_models`, a execução termina no passo 1.

## Parâmetros

| Parâmetro             | Descrição                                                                                       |
| --------------------- | ----------------------------------------------------------------------------------------------- |
| `cloud`               | Bucket, projeto e conexão BigLake das tabelas.                                                  |
| `dbt`                 | `DbtRun`: repositório das consultas (com `GITHUB_TOKEN` na URL), branch, `target` e `select`.   |
| `raw`                 | Bucket e prefixos dos arquivos brutos: `records_prefix` e `layout_prefix`.                      |
| `tables`              | `dataset_id`, `records` e `layout`.                                                             |
| `force_create_models` | Gera os modelos e roda o `dbt` mesmo sem ZIP novo.                                              |

## Pré-requisitos

- Toda coluna do layout precisa de uma linha na planilha `dicionario_colunas_cadunico`. Sem ela, o flow falha e lista as colunas.
- Toda versão de layout que está nos registros precisa de uma planilha no bucket de layout.
- `GITHUB_TOKEN` com permissão de escrita no repositório das consultas.

## Código

- `helpers/layout.py`: lê as planilhas e nomeia as colunas.
- `helpers/versions.py`: acompanha as colunas de uma versão para a seguinte.
- `helpers/models.py` e `queries/model.sql`: descrevem e escrevem os modelos `dbt`.
- `helpers/repository.py`: faz o `commit` e o `push` dos modelos.
- `helpers/prepare.py`, `helpers/files.py` e `helpers/storage.py`: baixam os arquivos brutos e os transformam em Parquet.
