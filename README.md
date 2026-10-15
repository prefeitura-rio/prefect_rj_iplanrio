# Pipelines - `Prefect`

Este repositório tem as pipelines `Prefect` da Prefeitura do Rio de Janeiro. Para contribuir, leia o [guia de contribuição](https://iplan-rio.mintlify.app/data-lake/prefect/construindo-uma-pipeline) e o [guia de estilo](STYLEGUIDE.md).

## Estrutura

```
pipelines/          uma pasta por pipeline: rj_<secretaria>__<pipeline>/
src/utils/          flows e tasks prontos, usados por várias pipelines
packages/toolkit/   biblioteca com o código de uso comum (DuckDB, GCS, BigQuery, dbt)
templates/          modelo para criar uma pipeline nova
Dockerfile          imagem base de todas as pipelines
pyproject.toml      dependências do repositório
```

Cada pipeline tem estes arquivos:

```
rj_sec__pipeline/
├── flow.py          o flow
├── prefect.yaml     o deployment: parâmetros, agenda e infraestrutura
├── pyproject.toml   as dependências da pipeline
└── Dockerfile       a imagem da pipeline
```

## Como funciona

- O repositório é um [`workspace` do `uv`](https://docs.astral.sh/uv/concepts/projects/workspaces/). Cada pasta de `pipelines/` e de `packages/` é um membro dele.
- Toda pipeline depende de `prefect_rj_iplanrio` (`{ workspace = true }`) e recebe as dependências base: `toolkit`, `prefect` e outras.
- As versões dos pacotes compartilhados (`pendulum`, `pandas`, `dbt-bigquery` e outros) ficam em `constraint-dependencies`, no `pyproject.toml` da raiz. A pipeline lista só o nome do pacote. Para subir uma versão, mude essa linha.
- O `toolkit` tem o código de uso comum. Veja o [README do toolkit](packages/toolkit/README.md).
- `src/utils/` tem os flows e as tasks prontos. A pipeline os importa do módulo `utils`:

  ```python
  from utils.flows import dump_database
  ```

- **Não crie `utils.py` nem `utils/` em uma pipeline.** O `Prefect` procura imports primeiro na pasta do `flow.py`, então um arquivo com esse nome esconde o módulo `utils`. Use `helpers.py` para o código só da pipeline.

## Criar uma pipeline

Instale o `uv` e rode:

```sh
uvx cookiecutter templates --output-dir=pipelines
```

O `cookiecutter` pergunta a `secretaria`, o nome da `pipeline`, o `tipo`, a `severidade` e o `code_owner`. O `tipo` escolhe o flow pronto:

| `tipo`          | O que faz                                                              |
| --------------- | ---------------------------------------------------------------------- |
| `dump_database` | Copia uma tabela ou consulta de Postgres, MySQL, SQL Server ou Oracle. |
| `ingest_api`    | Faz uma requisição a uma API e soma os registros à tabela.             |
| `dump_arcgis`   | Copia uma camada do `ArcGIS`.                                          |
| `dump_gsheet`   | Copia uma planilha do Google.                                          |

O modelo cria `flow.py` (duas linhas), `prefect.yaml` (`staging` e produção, com um `schedule` de exemplo), `pyproject.toml` e `Dockerfile`. Troque os valores de exemplo e pronto.

Se nenhum `tipo` serve, escreva um `flow.py` que chama as [tasks prontas](#tasks-prontas).

## Flows prontos

Os flows ficam em `src/utils/flows.py`. Todos gravam em uma tabela do BigQuery e avisam no Discord quando falham (se existir `DISCORD_WEBHOOK_URL`).

| Flow             | O que faz                                                                                 |
| ---------------- | ----------------------------------------------------------------------------------------- |
| `dump_database`  | Copia um banco para o BigQuery sem repetir dado. Com `dbt`, roda um comando `dbt` no fim. |
| `ingest_api`     | Faz uma requisição a uma API e carrega os registros.                                      |
| `dump_arcgis`    | Copia uma camada do `ArcGIS`. `crs` (como `EPSG:31983`) escolhe o sistema de coordenadas. |
| `dump_gsheet`    | Copia uma planilha. A primeira linha é o cabeçalho.                                       |
| `load_files`     | Carrega arquivos locais (`Parquet`, CSV ou JSON). Use em um flow que criou os arquivos.   |
| `compact_tables` | Junta os arquivos pequenos de tabelas que recebem dados a cada poucos minutos.            |
| `dbt_run`        | Clona o repositório de consultas e roda um comando `dbt`.                                 |

Os parâmetros são grupos com tipo (`src/utils/types.py`). O `Prefect` recusa uma chave que falta ou um tipo errado antes de rodar.

| Grupo      | O que tem                                                                          | Onde fica no `prefect.yaml` |
| ---------- | ---------------------------------------------------------------------------------- | --------------------------- |
| `database` | `type`, `host`, `port`, `database` e `secret_path` (o segredo com usuário e senha) | `deployment`                |
| `cloud`    | `bucket`, `project` e `connection` (a conexão `BigLake`)                           | `deployment`                |
| `table`    | `dataset_id` e `table_id`, a tabela no BigQuery                                    | `schedule`                  |
| `read`     | `table` (`esquema.tabela`) ou `query` (no dialeto do banco)                        | `schedule`                  |
| `options`  | `batches`, `filters`, `select` e `columns`                                         | `schedule`                  |

Cada `schedule` repete o grupo inteiro. O `Prefect` troca o grupo todo, e não só uma chave.

### Exemplo: banco para o BigQuery

```python
from utils.flows import dump_database

rj_secretaria__pipeline = dump_database.with_options(name="rj_secretaria__pipeline")
```

```yaml
deployments:
  - name: rj-secretaria--pipeline--prod
    entrypoint: pipelines/rj_secretaria__pipeline/flow.py:rj_secretaria__pipeline
    parameters:
      database:
        type: postgres # postgres, mysql, mssql ou oracle
        host: 10.0.0.1
        port: 5432
        database: app
        secret_path: /db-app
      cloud:
        bucket: bucket-prod
        project: project-prod
        connection: project-prod.us.biglake
    schedules:
      - cron: "0 1 * * *"
        timezone: America/Sao_Paulo
        slug: orders
        parameters:
          table:
            dataset_id: brutos_app
            table_id: orders
          read:
            table: public.orders # ou query: "SELECT ...", uma janela só
          dump_mode: append # ou overwrite
          options:
            batches:
              column: data_pedido
              width: month
              begin: "2024-01-01"
              lookback: 2
            filters: # só estas linhas
              - column: status
                op: "!="
                value: cancelado
            select: # só estas colunas
              - id
              - nome
              - valor
              - data_pedido
            columns:
              normalize_values: true # limpa o texto
              add_timestamp: true # grava a hora da carga
```

### Exemplo: API para o BigQuery

O `ingest_api` faz a requisição e tira a lista de registros da resposta. `records_key` é o caminho até a lista, como `data.items`. Deixe vazio se a resposta já é a lista.

`{NOME}` em `headers` e `params` vem do segredo de `secret_path`. O caminho `/api-x` lê a variável `API_X__NOME`. Assim o token nunca fica no `prefect.yaml`.

```yaml
schedules:
  - cron: "*/15 * * * *"
    timezone: America/Sao_Paulo
    slug: leituras
    parameters:
      table:
        dataset_id: brutos_api
        table_id: leituras
      url: https://api.exemplo.gov.br/leituras
      secret_path: /api-exemplo
      headers:
        Authorization: "Bearer {TOKEN}"
      records_key: data.items
```

- Em `append` (o padrão), cada execução soma os registros dela à tabela. Se a mesma execução roda de novo, ela não soma de novo, mesmo que a API devolva outros dados. Uma execução nova, mesmo manual, soma de novo.
- Em `overwrite`, os registros trocam a tabela.
- O flow faz uma requisição só. Para uma API com páginas, escreva um flow que pede cada página com `request_json` e carrega tudo com `load_files`.
- Cada execução cria arquivos pequenos. Agende `compact_tables` (por exemplo, à noite) para as tabelas que recebem dados a cada poucos minutos.

## Tasks prontas

As tasks ficam em `src/utils/tasks.py`. Use-as para montar um flow próprio.

| Task                                        | O que faz                                                                                                          |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `dump_db_to_gcs(database, table, read)`     | Copia uma tabela ou consulta de um banco para uma tabela `Delta` no bucket.                                        |
| `dump_arcgis_to_gcs(table, url)`            | Copia uma camada do `ArcGIS` para o bucket.                                                                        |
| `dump_gsheet_to_gcs(table, spreadsheet_id)` | Copia uma planilha para o bucket.                                                                                  |
| `upload_files_to_gcs(data_path, table)`     | Carrega arquivos locais para o bucket.                                                                             |
| `publish_table_in_bq(table, cloud)`         | Cria a tabela do BigQuery sobre a tabela do bucket, ou atualiza as colunas dela. Rode depois das tasks acima.      |
| `dump_bq_to_gcs(table, read, project=...)`  | Copia uma tabela ou consulta do BigQuery para `Parquet` no bucket, em lotes. Um `append` repetido soma de novo.    |
| `request_json(url)`                         | Faz uma requisição (`GET` por padrão) e devolve o JSON. Tenta de novo depois de timeout, erro de rede, 429 ou 5xx. |
| `write_records(records, folder)`            | Grava uma lista de objetos como linhas JSON, pronta para `upload_files_to_gcs`.                                    |
| `read_bq(query, project=...)`               | Roda uma consulta do BigQuery e devolve um `DataFrame` do `Polars`, todo na memória.                               |
| `run_bq_sql(sql, project=...)`              | Roda uma instrução do BigQuery, como um `MERGE`, e devolve quantas linhas ela mudou. Escreva o SQL na pipeline.    |
| `write_sheet(data_path, spreadsheet_id)`    | Escreve arquivos locais em uma planilha.                                                                           |
| `notify_discord(message)`                   | Posta uma mensagem no Discord.                                                                                     |
| `compact_delta_table(table, bucket=...)`    | Junta os arquivos pequenos de uma tabela `Delta`.                                                                  |
| `run_dbt(request)`                          | Roda um comando `dbt`.                                                                                             |

## Como o dump não repete dado

Tudo que vai de uma fonte para o bucket usa uma tabela `Delta`, que o BigQuery lê por uma conexão `BigLake`.

1. Uma **janela** é uma faixa fixa de uma coluna: um dia, mês ou ano de uma data, ou uma faixa de ids. Sem `batches`, a fonte inteira é uma janela. Com `batches`, as linhas com a coluna nula ficam na janela `null`, que toda execução lê de novo.
2. Para cada janela, o `DuckDB` lê a fonte com os `filters`, aplica `select` e `columns` e grava um `Parquet` local. Uma **impressão digital** do arquivo (a contagem de linhas e duas somas de `md5`) fica no registro da tabela `Delta`.
3. Se a impressão digital é igual à do registro, nada é gravado. Se mudou, a janela é trocada em um só `commit`. Uma janela que ficou vazia na fonte perde as linhas.
4. Uma janela nova sempre é lida. `lookback: 2` lê de novo só as duas últimas janelas já carregadas.
5. Se `filters`, `select`, `columns` ou `batches` mudam, todas as janelas são carregadas de novo. O intervalo (`begin` e `end`) e o `lookback` não contam.
6. `append` falha com `SchemaMismatchError` se as colunas mudarem. `overwrite` grava todas as janelas, apaga as que saíram do intervalo e aceita colunas novas.
7. Com `add_timestamp`, cada linha ganha a coluna `_prefect_extracted_at`: a hora em que a janela foi gravada (`TIMESTAMP` em UTC). Uma janela que não mudou guarda a hora antiga.

## Limites e cuidados

- Uma janela que não mudou ainda é lida na fonte, porque a impressão digital precisa das linhas. Use `lookback` e faça uma carga completa às vezes.
- Uma consulta nativa (`read.query`) é uma janela só e roda duas vezes: uma para descrever as colunas e outra para ler. O Oracle é a exceção: ele aplica filtros e janelas, em tabela ou consulta.
- O `deltalake` lê a tabela pelo esquema `gcs://` (OpenDAL), que é o de `Destination.uri`. Esse esquema lê todo tipo de ADC: servidor de metadados do GKE, `service account`, credencial de usuário e `external_account` (federação). O esquema `gs://` não lê o `external_account`.
- O `BigQuery` só aceita `gs://`, então o `toolkit` converte o `gcs://` na hora de criar a tabela externa (`build_source_uri`).
- Para conferir a credencial de um pod, rode nele `python -c "from deltalake import DeltaTable; print(DeltaTable.is_deltatable('gcs://<bucket>/staging/x'))"`. O resultado `False` sem erro quer dizer que a autenticação funciona.
- A planilha precisa estar compartilhada com a `service account`. O `DuckDB` lê todo número da planilha como `DOUBLE`.
- Os tipos que o `Delta` não guarda e as regras de cada banco estão no [README do toolkit](packages/toolkit/README.md#bancos-tipos-e-limites).

## Desenvolvimento

O ambiente usa [`devenv`](https://devenv.sh) (Python 3.14, `uv`, `nushell`, `duckdb` e `gcloud`). Para entrar nele, rode `devenv shell`.

```sh
devenv --profile checks tasks run lint:all test:all   # o mesmo que o CI roda
```

O `lint:all` roda os linters, os tipos, o SQL, as versões de dependência e os workflows. O `test:all` roda os testes da raiz e do toolkit. O `devenv` roda em paralelo o que não depende um do outro e pula o que não mudou desde a última execução. As tasks chamam os scripts `Nushell` de `scripts/`.

## Testes

```sh
pytest              # sem rede nem contêiner
pytest -n 4         # em quatro processos, como o `test:root`
```

Os testes não usam a nuvem de verdade. O GCS é um servidor falso com a API XML, o BigQuery lê a tabela `Delta` com as regras que o serviço real mostrou, e o banco é um `DuckDB` em memória. O `deltalake`, o `DuckDB` e o `Polars` são os de verdade. Por isso um teste não pega uma mudança do serviço real: confira com uma execução em um ambiente de staging.

O `pytest-xdist` roda os testes da raiz em quatro processos, e cada processo sobe o próprio servidor GCS falso (as portas são livres, então não há conflito). Com mais de quatro o ganho some, porque cada processo importa o código de novo. Os testes do `toolkit` rodam em um processo só: o ganho lá é de 2 s.

O servidor GCS falso não tem como receber o `gcs://` (o OpenDAL não aceita um endereço falso). Por isso a fixture `cloud` de `tests/test_cloud.py` faz `Destination.uri` devolver `gs://` nos testes. O `gcs://` em si só é conferido pelo teste de `Destination.uri` e pelo de `build_source_uri`.

Flows não têm teste, nem os de `src/utils/flows.py`: um flow só liga componentes, e confiamos nos testes de cada componente (`utils.py` e o `toolkit`).

## Pipelines

- O `prefect.yaml` define o `deployment`: parâmetros, agenda, infraestrutura e `secretName`. O CI usa esse arquivo para registrar os flows. Veja a [documentação do `Prefect`](https://docs.prefect.io/v3/how-to-guides/deployments/prefect-yaml).
- O código e as dependências só da pipeline ficam no `Dockerfile` e no `pyproject.toml` dela.
- `secretName` é o `Secret` do Kubernetes que o job monta. Use `prefect-jobs-secrets` em produção e `prefect-jobs-secrets-staging` em `staging`.
- Os nomes usam só letras minúsculas, números e `_`:
  - pasta: `rj_<secretaria>__<pipeline>`
  - `deployment`: `rj-<secretaria>--<pipeline>--<ambiente>`, como `rj-segovi--dump-db-1746--staging`
- Work pools:
  - `default-pool`: trabalho geral.
  - `onprem-pool`: bancos e sistemas internos da `IplanRio`, que exigem VPN.

## CI/CD

O `GitHub Actions` roda as tasks do `devenv` e os scripts `Nushell` de `scripts/`. Assim o que roda no CI roda igual na sua máquina.

| Workflow                | Quando roda                                                                                                                                          | O que faz                                                                                                                                                                                    |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ci.yaml`               | `pull request` e `push` na `master`                                                                                                                  | `devenv --profile checks tasks run lint:all test:all`. O `devenv` já roda as tasks independentes em paralelo.                                                                                |
| `deploy.yaml`           | `push` na `master` (prod) ou em `staging/*` (staging), quando `pipelines/**`, `src/**`, `packages/**`, `pyproject.toml` ou `uv.lock` muda; ou manual | Escolhe as pipelines com `scripts/deploy.nu plan` e registra cada uma no `Prefect` com `scripts/deploy.nu run`, quatro por vez.                                                              |
| `build-root-image.yaml` | Mudança no `Dockerfile` ou no `openssl.cnf` da raiz, na `master`; ou manual                                                                          | Publica `ghcr.io/<repositório>:latest` e chama `deploy.yaml` para todas as pipelines, em staging e em prod (os dois ambientes em paralelo). Os dois ambientes não esperam um `pull request`. |
| `sast.yaml`             | `pull request` e `push` na `master`                                                                                                                  | Análise de segurança.                                                                                                                                                                        |

Como o `deploy.yaml` funciona:

- **`plan`:** escolhe as pipelines que têm um `deployment` do ambiente, isto é, um `name` que termina em `--prod` ou `--staging`. O `plan` lê o `prefect.yaml` como YAML.
  - Se mudou um arquivo de `pipelines/<nome>/`, só essa pipeline entra.
  - Se mudou `src/`, `packages/`, `pyproject.toml` ou `uv.lock`, todas as pipelines entram, porque cada imagem tem esse código dentro. Arquivos `.md` não contam.
  - Em `staging/*`, só segue se a branch tem um `pull request` aberto para a `master`.
  - Na `master`, se o mesmo `push` muda o `Dockerfile` ou o `openssl.cnf`, o `plan` não faz nada. O `build-root-image.yaml` publica a imagem nova e depois faz o deploy de todas as pipelines.
- **`deploy`:** um job por pipeline, cada um no próprio runner, com quatro em paralelo. Uma falha não cancela as outras. O script tenta de novo (duas vezes) só quando o erro é de rede.
- **Manual:** informe o ambiente e, se quiser, uma `pipeline` ou `all`.
- **Permissões e tokens:** cada workflow começa com `permissions: {}` e cada job pede só o que usa. O login no `ghcr.io` usa o `GITHUB_TOKEN`.
- **Versões das actions:** todas ficam fixas por `hash` de commit, e o `Renovate` (`renovate.json`) atualiza.

Para testar o plano na sua máquina:

```sh
devenv --profile deploy shell -- nu scripts/deploy.nu plan --environment prod --all
devenv --profile deploy shell -- nu scripts/deploy.nu run rj_smdue__dump_db_sislic --environment prod --dry-run
```

## Problemas comuns

| Erro                                                       | Causa e solução                                                                                                                                                    |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `uv sync --package` falha com "package not found"          | O `pyproject.toml` da pipeline não declara `prefect_rj_iplanrio`. Ponha-o em `dependencies` e `prefect_rj_iplanrio = { workspace = true }` em `[tool.uv.sources]`. |
| `No module named 'utils.flows'`                            | Um `utils.py` ou uma pasta `utils/` da pipeline esconde o módulo compartilhado. Renomeie o código local (por exemplo, `helpers.py`).                               |
| O build do Docker falha com `cx-oracle` ou `pkg_resources` | O `cx-oracle` não funciona no Python 3.14. O `pyproject.toml` da raiz já usa `oracledb`. Rode `uv lock`.                                                           |
| O `container` não acha `utils` ou `toolkit`                | O `Dockerfile` não copia `src/` e `packages/`. Copie as duas pastas, como no modelo.                                                                               |
| O `uv.lock` está desatualizado ao criar uma pipeline       | Rode `uv lock` na raiz do repositório.                                                                                                                             |
