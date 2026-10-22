# Pipelines - `Prefect`

Este repositório tem as pipelines `Prefect` da Prefeitura do Rio de Janeiro. Para contribuir, leia o [guia de contribuição](https://iplan-rio.mintlify.app/data-lake/prefect/construindo-uma-pipeline) e o [guia de estilo](STYLEGUIDE.md).

## Estrutura

```
pipelines/          uma pasta por pipeline: rj_<secretaria>__<pipeline>/
src/utils/          flows e tasks prontos, usados por várias pipelines
packages/toolkit/   biblioteca com o código de uso comum (DuckDB, GCS, BigQuery, dbt)
templates/          modelo para criar uma pipeline nova
scripts/            scripts Nushell: verificações e deploy
tests/              testes de `src/utils` e testes de paridade
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

| Flow             | O que faz                                                                                    |
| ---------------- | -------------------------------------------------------------------------------------------- |
| `dump_database`  | Copia um banco para o BigQuery sem repetir dado. Com `dbt`, roda um comando `dbt` no fim.    |
| `ingest_api`     | Faz uma requisição a uma API e carrega os registros.                                         |
| `dump_arcgis`    | Copia uma camada do `ArcGIS`. `crs` (como `EPSG:31983`) escolhe o sistema de coordenadas.    |
| `dump_gsheet`    | Copia uma planilha. A primeira linha é o cabeçalho.                                          |
| `load_files`     | Carrega arquivos locais (`Parquet`, CSV ou JSON). Use em um flow que criou os arquivos.      |
| `maintain_delta` | Junta os arquivos pequenos, grava o `checkpoint`, limpa e atualiza todas as tabelas `Delta`. |
| `dbt_run`        | Clona o repositório de consultas e roda um comando `dbt`. `report` avisa o Discord.          |

No `dbt_run`, `request` é o grupo `DbtRun`: `repository`, `command`, `target`, `select`, `exclude`, `flag` e `full_refresh`. Um repositório privado leva `GITHUB_TOKEN` no lugar do token na URL, como em `https://GITHUB_TOKEN@github.com/org/queries.git`, e a variável de ambiente `GITHUB_TOKEN` o preenche. `flag` são os demais argumentos do `dbt`, separados como no shell. Com `report: true`, o flow posta o resultado no Discord (nós que falharam ou avisaram), se `DISCORD_WEBHOOK_URL` existe.

Os parâmetros são grupos com tipo (`src/utils/types.py`). O `Prefect` recusa uma chave que falta ou um tipo errado antes de rodar.

| Grupo      | O que tem                                                                                                                                                                                                                                                                             | Onde fica no `prefect.yaml` |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------- |
| `database` | `type`, `host`, `port`, `database` e `options` (opcional, da fonte). O usuário e a senha vêm de `DB_USERNAME` e `DB_PASSWORD`. `host` e `database` aceitam `${NOME}`, a variável de ambiente `NOME` (por exemplo `host: ${TAXIRIO_MONGODB_HOST}`)                                                                                                                                                         | `deployment`                |
| `cloud`    | `bucket`, `project` e `connection` (a conexão `BigLake`)                                                                                                                                                                                                                              | `deployment`                |
| `table`    | `dataset_id` e `table_id`, a tabela no BigQuery                                                                                                                                                                                                                                       | `schedule`                  |
| `read`     | `table` (`esquema.tabela`) ou `query` (no dialeto do banco). No CouchDB, `table` é a base, `query` um seletor Mango e `since` uma sequência de `_changes`. No MongoDB, `table` é a coleção, `query` um filtro `JSON` e `pipeline` o nome de um arquivo `JSON` da pasta `pipelines/` da pipeline, com a lista de estágios de agregação (por exemplo `pipeline: payment-methods` lê `pipelines/rj_iplanrio__taxirio/pipelines/payment-methods.json`) | `schedule`                  |
| `options`  | `chunks`, `filters`, `select`, `columns`, `batch_size` e `concurrency`                                                                                                                                                                                                                | `schedule`                  |

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
        type: postgres # postgres, mysql, mssql, oracle, couchdb ou mongodb
        host: 10.0.0.1
        port: 5432
        database: app
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
            table: public.orders # ou query: "SELECT ...", um chunk só
          dump_mode: append # ou overwrite
          options:
            chunks:
              column: data_pedido
              width: month
              begin: "2024-01-01"
              lookback: 2
            filters: # só estas linhas: um predicado SQL por item, ligados por AND
              - "status <> 'cancelado'"
              - "valor > 0 OR desconto IS NOT NULL"
            select: # só estas colunas
              - id
              - nome
              - valor
              - data_pedido
            columns:
              normalize_values: true # limpa o texto
              add_timestamp: true # grava a hora da carga
```

### Filtros e tamanho do lote

`filters` é uma lista, e cada item é um filtro no idioma da fonte. Os itens se ligam por `AND`, e um `OR` fica dentro do seu item. O item vai entre parênteses, e não pode ter `;` nem comentário (`--` ou `/*`).

| Fonte                        | O item é                                  | Exemplo                   |
| ---------------------------- | ----------------------------------------- | ------------------------- |
| `postgres`, `mysql`, `mssql` | um predicado em SQL do `DuckDB`           | `"status <> 'cancelado'"` |
| `oracle`                     | um predicado em SQL do Oracle             | `"STATUS <> 'CANCELADO'"` |
| `couchdb`                    | um seletor Mango, em texto `JSON`         | `'{"situacao": "ativo"}'` |
| `mongodb`                    | um documento de consulta, em texto `JSON` | `'{"situacao": "ativo"}'` |

Os nomes das colunas são os da fonte, como em `select`. No CouchDB, `chunks.column` precisa ser `id`, e os filtros não vão com `read.query` nem com `read.since`.

Uma chave de `options` que não existe é um erro, para um nome errado nunca virar o padrão.

`batch_size` é o número de linhas de cada leitura. Vale para o Oracle (padrão 50 000), para o CouchDB (documentos por página, padrão `COUCHDB_PAGE_SIZE`) e para o MongoDB (documentos por página, padrão 50 000). Os outros bancos passam pelo `DuckDB`, que lê em fluxo, e um `batch_size` neles é um erro. O `batch_size` não muda o conteúdo de um chunk, então não conta para recarregar os chunks.

`concurrency` é o número de chunks que o CouchDB e o MongoDB leem ao mesmo tempo (padrão 2). Cada leitor abre o seu próprio cliente e o seu próprio `DuckDB`, então a memória e o disco local crescem com o número. Os `commits` na tabela `Delta` continuam um por vez. Os outros bancos leem um chunk por vez, e um `concurrency` neles é um erro. O `concurrency` não muda o conteúdo de um chunk, então não conta para recarregar os chunks.

```yaml
# CouchDB: quatro chunks ao mesmo tempo
options:
  chunks:
    column: id
    width: 100000000
    begin: 0
    end: 99999999999
  concurrency: 4
```

```yaml
# Oracle
read:
  table: APP.PEDIDOS
options:
  chunks:
    column: DATA_PEDIDO
    width: month
    begin: "2024-01-01"
  filters:
    - "STATUS <> 'CANCELADO'"
  batch_size: 20000
```

```yaml
# CouchDB
read:
  table: chcpf_bcadastros
options:
  chunks:
    column: id
    width: 100000000
    begin: 0
    end: 99999999999
  filters:
    - '{"situacao": "ativo"}'
  batch_size: 5000
```

O MongoDB pede o extra do toolkit: `uv add "toolkit[mongodb]"` no pipeline.

```yaml
# MongoDB (a coleção e o esquema do PyMongoArrow, com o campo _id)
database:
  type: mongodb
  host: 10.0.0.5
  port: 27017
  database: app
  options:
    shape: utils.taxirio:USERS
    auth_source: admin
read:
  table: users
options:
  chunks:
    column: created_at
    width: month
    begin: "2024-01-01"
  filters:
    - '{"status": "active"}'
  batch_size: 20000
```

### Exemplo: API para o BigQuery

O `ingest_api` faz a requisição e tira a lista de registros da resposta. `records_key` é o caminho até a lista, como `data.items`. Deixe vazio se a resposta já é a lista. `records_schema` é o caminho `módulo:nome` de um modelo `Pydantic`, de uma `dataclass` ou de um `TypedDict` a que todo registro precisa obedecer: um registro fora do esquema para a execução antes de gravar qualquer coisa, e a mensagem diz o índice do registro e o campo, nunca o valor. Os registros são gravados como a API os enviou.

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
      headers:
        Accept: application/json
      records_key: data.items
      records_schema: utils.schemas:Leitura
```

- Em `append` (o padrão), cada execução soma os registros dela à tabela. Se a mesma execução roda de novo, ela não soma de novo, mesmo que a API devolva outros dados. Uma execução nova, mesmo manual, soma de novo.
- Em `overwrite`, os registros trocam a tabela.
- O flow faz uma requisição só. Para uma API com páginas, escreva um flow que pede cada página com `request_json` e carrega tudo com `load_files`.
- Cada execução cria arquivos pequenos. A pipeline `rj_iplanrio__maintain_delta` junta os arquivos de todas as tabelas toda semana. Veja [Manutenção das tabelas](#manutenção-das-tabelas).

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
| `list_delta_tables(bucket)`                 | Lista as tabelas `Delta` da pasta `staging/` do bucket.                                                            |
| `maintain_delta_table(table, bucket=...)`   | Junta os arquivos pequenos de uma tabela `Delta`, grava o `checkpoint` e limpa os arquivos sem uso.                |
| `run_dbt(request)`                          | Clona o repositório e roda um comando `dbt`. Com `report`, posta o resultado no Discord.                           |

## Como o dump não repete dado

Tudo que vai de uma fonte para o bucket usa uma tabela `Delta`, que o BigQuery lê por uma conexão `BigLake`.

1. Um **chunk** é uma faixa fixa de uma coluna: um dia, mês ou ano de uma data, ou uma faixa de ids. Sem `chunks`, a fonte inteira é um chunk. Com `chunks`, as linhas com a coluna nula ficam no chunk `null`, que toda execução lê de novo.
2. Para cada chunk, o `DuckDB` lê a fonte com os `filters`, aplica `select` e `columns` e grava um `Parquet` local. Uma **impressão digital** do arquivo (a contagem de linhas e duas somas de `md5`) fica no registro da tabela `Delta`.
3. Se a impressão digital é igual à do registro, nada é gravado. Se mudou, o chunk é trocado em um só `commit`. Um chunk que ficou vazio na fonte perde as linhas.
4. Um chunk novo sempre é lido. `lookback: 2` lê de novo só os dois últimos chunks já carregados.
5. Se `read`, `filters`, `select`, `columns` ou `chunks` (a coluna ou a largura) mudam, todos os chunks são carregados de novo. O intervalo (`begin` e `end`), o `lookback`, o `batch_size` e o `concurrency` não contam.
6. `append` falha com `SchemaMismatchError` se as colunas mudarem. `overwrite` grava todos os chunks, apaga os que saíram do intervalo e aceita colunas novas.
7. Com `add_timestamp`, cada linha ganha a coluna `_prefect_extracted_at`: a hora em que o chunk foi gravado (`TIMESTAMP` em UTC). Um chunk que não mudou guarda a hora antiga.

## Manutenção das tabelas

O flow `maintain_delta` roda na pipeline `rj_iplanrio__maintain_delta`, todo domingo às 03:00. Ele nunca muda as linhas.

1. Procura toda pasta `staging/<dataset>/<tabela>` que tem um `_delta_log`.
2. Em cada tabela, junta os arquivos pequenos, grava o `checkpoint`, apaga os arquivos que nenhuma versão lê (depois de 168 horas) e atualiza a tabela do BigQuery.
3. Posta no Discord, se `DISCORD_WEBHOOK_URL` existe, as tabelas que falharam, as tabelas com chunks de dois layouts e quantos arquivos a execução juntou.
4. Uma tabela que falha não para as outras. A execução falha no fim, com a lista.

O flow não corrige estes casos. Ele só avisa do segundo.

- Um chunk antigo que mudou na fonte e está fora do `lookback`. Faça uma execução com `overwrite` ou aumente o `lookback`.
- Linhas repetidas depois de mudar a largura ou a coluna de `chunks` em `append`. O aviso é `all next to chunk keys` ou `dates next to numbers`. Rode uma vez com `overwrite`.
- Duas capturas de `capture` que rodam ao mesmo tempo.

`exclude` pula tabelas, no formato `dataset_id.table_id`. Os ambientes `staging` e `prod` usam o mesmo bucket, então só `prod` tem horário.

## Limites e cuidados

- Mudar a coluna ou a largura de `chunks` em `append` cria chunks com outros nomes e deixa os antigos na tabela, então as linhas ficam repetidas. Rode uma vez com `overwrite`. O flow `maintain_delta` avisa quando vê os dois layouts.
- Um chunk que não mudou ainda é lido na fonte, porque a impressão digital precisa das linhas. Use `lookback` e faça uma carga completa às vezes.
- Uma consulta nativa (`read.query`) é um chunk só e roda duas vezes: uma para descrever as colunas e outra para ler. O Oracle é a exceção: ele aplica filtros e chunks, em tabela ou consulta.
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

`nu scripts/checks.nu lint imports` lista os imports de módulos que saíram (`toolkit.pipelines_utils`, `prefect_rj_iplanrio` e outros) nas pipelines, e falha se achar algum. `nu scripts/checks.nu lint env` falha quando um campo de `Settings` foge da regra de nome (Seção 5.11 do `STYLEGUIDE.md`) ou quando uma pipeline lê uma variável de ambiente à mão. O `lint:all` roda os linters, os tipos, o SQL, as versões de dependência, os imports, as variáveis de ambiente e os workflows. O `test:all` roda os testes da raiz e do toolkit. O `devenv` roda em paralelo o que não depende um do outro e pula o que não mudou desde a última execução. As tasks chamam os scripts `Nushell` de `scripts/`.

## Testes

```sh
pytest              # sem rede nem contêiner
pytest -n 4         # em quatro processos, como o `test:root`
```

Os testes não usam a nuvem de verdade, mas os de `tests/test_cloud.py` precisam de uma credencial do Google: o `deltalake` pede um token mesmo com o servidor GCS falso, e sem credencial os 13 testes falham depois de 4 minutos. Uma credencial que o Google recusa (`GOOGLE_APPLICATION_CREDENTIALS` com um arquivo inválido ou vencido) falha com `invalid_grant`. O marcador `cloud` fica desligado por padrão, então o CI não roda esses testes. Rode `uv run pytest -m cloud tests/test_cloud.py` com uma credencial válida. O GCS é um servidor falso com a API XML, o BigQuery lê a tabela `Delta` com as regras que o serviço real mostrou, e o banco é um `DuckDB` em memória. O `deltalake`, o `DuckDB` e o `Polars` são os de verdade. Por isso um teste não pega uma mudança do serviço real: confira com uma execução em um ambiente de staging.

O `pytest-xdist` roda os testes da raiz em quatro processos, e cada processo sobe o próprio servidor GCS falso (as portas são livres, então não há conflito). Com mais de quatro o ganho some, porque cada processo importa o código de novo. Os testes do `toolkit` rodam em um processo só: o ganho lá é de 2 s.

O servidor GCS falso não tem como receber o `gcs://` (o OpenDAL não aceita um endereço falso). Por isso a fixture `cloud` de `tests/test_cloud.py` faz `Destination.uri` devolver `gs://` nos testes. O `gcs://` em si só é conferido pelo teste de `Destination.uri` e pelo de `build_source_uri`.

Flows não têm teste, nem os de `src/utils/flows.py`: um flow só liga componentes, e confiamos nos testes de cada componente (`utils.py` e o `toolkit`).

Os testes de paridade (`tests/parity/`) rodam os seis motores de verdade: `Postgres`, `MySQL`, `SQL Server`, `Oracle`, `CouchDB` e `MongoDB`. O marcador `parity` fica desligado por padrão. `nu tests/parity/engines.nu up` sobe os motores (os quatro primeiros pelo `nix`, `SQL Server` e `Oracle` pelo `podman`), com portas livres e senhas geradas em `/tmp/toolkit-parity/env.json` (ou em `PARITY_LAB`). Depois, `uv run pytest -m parity tests/parity` confere que cada motor devolve as mesmas linhas, os mesmos tipos e os mesmos chunks. `nu tests/parity/engines.nu down` derruba tudo.

## Pipelines

- O `prefect.yaml` define o `deployment`: parâmetros, agenda, infraestrutura e `secretName`. O CI usa esse arquivo para registrar os flows. Veja a [documentação do `Prefect`](https://docs.prefect.io/v3/how-to-guides/deployments/prefect-yaml).
- O código e as dependências só da pipeline ficam no `Dockerfile` e no `pyproject.toml` dela.
- `secretName` é o `Secret` do Kubernetes que o job monta. Use `prefect-jobs-secrets` em produção e `prefect-jobs-secrets-staging` em `staging`.
- Os nomes usam só letras minúsculas, números e `_`:
  - pasta: `rj_<secretaria>__<pipeline>`
  - `deployment`: `rj-<secretaria>--<pipeline>--<ambiente>`, como `rj-segovi--dump-db-1746--staging`
- `DB_USERNAME` e `DB_PASSWORD` vêm do `Secret` do `deployment`. Bancos com logins diferentes precisam de `secretName` diferentes.
- O `staging` e a `prod` usam o mesmo `bucket` e o mesmo projeto. Um `overwrite` manual no `staging` troca a tabela da `prod`.
- O `staging` não tem `schedule`. Para rodar à mão, copie os `parameters` de um `schedule` da `prod`.
- Work pools:
  - `default-pool`: trabalho geral.
  - `onprem-pool`: bancos e sistemas internos da `IplanRio`, que exigem VPN.

## CI/CD

O `GitHub Actions` roda as tasks do `devenv` e os scripts `Nushell` de `scripts/`. Assim o que roda no CI roda igual na sua máquina.

| Workflow                | Quando roda                                                                                                                                          | O que faz                                                                                                                                                                                    |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ci.yaml`               | `pull request` e `push` na `master`, na `main` e em `staging/**`                                                                                       | Dois jobs: `devenv --profile checks tasks run lint:all test:all` e os testes de paridade com `Postgres`, `MySQL`, `CouchDB` e `MongoDB` reais. O `devenv` já roda as tasks independentes em paralelo. |
| `deploy.yaml`           | `push` na `master` (prod) ou em `staging/*` (staging), quando `pipelines/**`, `src/**`, `packages/**`, `pyproject.toml` ou `uv.lock` muda; ou manual | Escolhe as pipelines com `scripts/deploy.nu plan` e registra cada uma no `Prefect` com `scripts/deploy.nu run`, quatro por vez.                                                              |
| `build-root-image.yaml` | Mudança no `Dockerfile` ou no `openssl.cnf` da raiz, na `master`; ou manual                                                                          | Publica `ghcr.io/<repositório>:latest` e chama `deploy.yaml` para todas as pipelines, em staging e em prod (os dois ambientes em paralelo). Os dois ambientes não esperam um `pull request`. |
| `sast.yaml`             | `pull request` e `push` na `master`                                                                                                                  | Análise de segurança.                                                                                                                                                                        |

O `deploy.yaml` só roda quando a variável do repositório `DEPLOY_ENABLED` vale `true`. Sem ela, nada faz deploy: nem um `push`, nem a execução manual, nem o `build-root-image.yaml`. Crie a variável em _Settings > Secrets and variables > Actions > Variables_ para ligar o deploy, e apague-a para desligar.

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
