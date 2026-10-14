# Pipelines - Prefect

Este repositório contém pipelines desenvolvidas com Prefect para automação de fluxos de dados da Prefeitura do Rio de Janeiro.

## Contribuindo com o projeto

Se você deseja contribuir com o projeto, acesse nossa [guia de contribuição](https://iplan-rio.mintlify.app/data-lake/prefect/construindo-uma-pipeline).

## Estrutura do projeto

### Raiz do repositório

```
pipelines/
├── rj_sec__pipeline1/
├── rj_sec__pipeline2/
└── ...
src/
└── prefect_rj_iplanrio/
    └── __init__.py
packages/
└── toolkit/
    └── toolkit/
Dockerfile
pyproject.toml
uv.lock
```

### Pipeline

```
rj_sec__pipeline
├── Dockerfile
├── flow.py
├── prefect.yaml
├── pyproject.toml
└── ...
```

## Como usar este repositório

Este repositório utiliza um modelo monorepo para gerenciar múltiplas pipelines Prefect de forma centralizada e padronizada:

- **Dockerfile base**: o arquivo `Dockerfile` na raiz define a imagem base utilizada por todas as pipelines. Ele inclui as dependências essenciais para execução dos fluxos, como Python, Prefect, drivers de banco de dados e ferramentas auxiliares. Cada pipeline pode customizar sua própria imagem a partir desse Dockerfile base, garantindo consistência e facilidade de manutenção.

- **`pyproject.toml` centralizado com uv workspaces**: o `pyproject.toml` na raiz do projeto gerencia as dependências Python de todas as pipelines utilizando [uv workspaces](https://docs.astral.sh/uv/concepts/projects/workspaces/).
  - Cada subdiretório em `pipelines/` representa um módulo Python independente, mas todos são importados automaticamente como membros do workspace.
  - As pipelines declaram `prefect_rj_iplanrio` como dependência com `{ workspace = true }`, herdando automaticamente as dependências base (`toolkit`, `prefect`, `pandas`, etc.).
  - Novas pipelines adicionadas à pasta `pipelines/` são automaticamente reconhecidas e integradas ao workspace, sem necessidade de configuração manual adicional.

- **Pacote `toolkit`**: o diretório `packages/toolkit/` é um membro do workspace com os primitivos compartilhados (execução de SQL no DuckDB, staging no GCS, tabelas externas no BigQuery, extração de bancos). As regras estão em [`packages/toolkit/STYLEGUIDE.md`](packages/toolkit/STYLEGUIDE.md).

- **Código compartilhado**: o diretório `src/prefect_rj_iplanrio/` contém código Python compartilhado entre as pipelines.
  - Funções utilitárias, classes base, helpers e lógica comum podem ser adicionados neste pacote.
  - Todas as pipelines que dependem de `prefect_rj_iplanrio` podem importar este código diretamente:
    ```python
    from prefect_rj_iplanrio import minha_funcao_compartilhada
    ```
  - Isso evita duplicação de código e facilita a manutenção de funcionalidades reutilizáveis.

Esse modelo garante reprodutibilidade, isolamento e facilidade de evolução para todas as pipelines do repositório.

### Tarefas e fluxos compartilhados

`src/prefect_rj_iplanrio/tasks.py` e `flows.py` têm o que mais de uma pipeline usa, sobre o `toolkit`. Os fluxos são assíncronos, e os parâmetros de cada ambiente (`bucket`, `project`, `biglake_connection`) vêm do `prefect.yaml`. Os parâmetros são grupos tipados (`Database`, `Cloud`, `Table`, `Read` e `DumpOptions`, em `types.py`): o Prefect recusa uma chave que falta, um erro de digitação ou um tipo errado antes de rodar. O Prefect troca o grupo inteiro, e não só a chave, quando um `schedule` repete um parâmetro do `deployment`, então `database` e `cloud` ficam no `deployment` e `table`, `read` e `options` em cada `schedule`.

| Item                                             | O que faz                                                                                                                                                                                                                  |
| ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dump_db_to_gcs(database, table, read, ...)`     | Copia uma tabela ou uma consulta de `postgres`, `mysql`, `mssql` ou `oracle` para uma tabela `Delta` no bucket, uma janela por vez, sem repetir dado que não mudou.                                                        |
| `publish_table_in_bq(table, cloud)`              | Cria a tabela do BigQuery sobre a tabela `Delta` do bucket, ou atualiza o esquema dela. Exige `biglake_connection`.                                                                                                        |
| `dump_bq_to_gcs(table, read, ...)`               | Copia uma tabela ou consulta do BigQuery para `Parquet` no bucket, em lotes simples: o `DuckDB` lê `batch_size` linhas por vez e cada lote vira um arquivo. Não tem ledger, e um `append` repetido soma as linhas de novo. |
| `dump_arcgis_to_gcs(table, url, ...)`            | Carrega uma camada do ArcGIS REST em uma tabela `Delta`, como uma janela só.                                                                                                                                               |
| `dump_gsheet_to_gcs(table, spreadsheet_id, ...)` | Carrega uma planilha do Google em uma tabela `Delta`, como uma janela só.                                                                                                                                                  |
| `upload_files_to_gcs(data_path, table, ...)`     | Carrega arquivos locais (`Parquet`, CSV como texto ou JSON) em uma tabela `Delta`, com os mesmos `options` do dump de um banco.                                                                                            |
| `write_sheet(data_path, spreadsheet_id, ...)`    | Escreve arquivos locais em uma planilha do Google.                                                                                                                                                                         |
| `fetch_json(url, ...)`                           | Lê o JSON de uma URL. Tenta de novo depois de timeout, erro de rede, 429 ou 5xx, e falha na hora com um 4xx.                                                                                                               |
| `run_dbt(request)`                               | Roda um comando `dbt` de um repositório de consultas.                                                                                                                                                                      |
| `dump_database(...)` (fluxo)                     | Junta `dump_db_to_gcs` e `publish_table_in_bq`, com duas tentativas em cada etapa. Uma tentativa nova é segura: o que já foi gravado não se repete. Uma falha avisa no Discord se houver `DISCORD_WEBHOOK_URL`.            |
| `dbt_run(request)` (fluxo)                       | Clona o repositório, roda `dbt deps` e o comando de `request`. Um `profiles.yml` no projeto vale mais que o de `~/.dbt`.                                                                                                   |

Uma pipeline de dump só precisa de um `flow.py` de duas linhas e dos parâmetros no `prefect.yaml`:

```python
from prefect_rj_iplanrio.flows import dump_database

rj_secretaria__pipeline = dump_database.with_options(name="rj_secretaria__pipeline")
```

```yaml
deployments:
  - name: rj-secretaria--pipeline--prod
    entrypoint: pipelines/rj_secretaria__pipeline/flow.py:rj_secretaria__pipeline
    parameters:
      database:
        db_type: postgres
        host: 10.0.0.1
        port: 5432
        database: app
        secret_path: /db-app
      cloud:
        bucket: bucket-prod
        project: project-prod
        biglake_connection: project-prod.us.biglake
    schedules:
      - interval: 86400
        slug: orders
        parameters:
          table: { dataset_id: brutos_app, table_id: orders }
          read: { table: public.orders } # ou { query: "SELECT ..." }, uma janela só
          dump_mode: append
          options:
            batches: { column: data_pedido, width: month, begin: "2024-01-01", lookback: 2 }
            filters: [{ column: status, op: "!=", value: cancelado }] # linhas
            select: [id, nome, valor, data_pedido] # colunas
            columns: { normalize_values: true, transforms: { cpf: normalize_cpf } }
```

**Como o dump não repete dado**

1. Uma janela é uma faixa fixa de uma coluna: um `day`, `month` ou `year` de uma data, ou um número de ids (`width: 1000000`). A chave é estável, por exemplo `2024-03-01`. Sem `batches`, a fonte inteira é uma janela só. Com `batches`, as linhas cuja coluna é `NULL` ficam numa última janela, `null`, que toda execução relê (nenhuma faixa as contém).
2. Para cada janela, o `DuckDB` lê a fonte já com a janela e os `filters` (o banco recebe o filtro, porque `read.table` é uma tabela anexada), aplica `select` e `columns`, e grava um `Parquet` local. A impressão digital desse arquivo (a contagem de linhas e duas somas do `md5` de cada linha, que não dependem da ordem) vai para o ledger da tabela `Delta`, com o nome `<hash da config>:<janela>`.
3. Se a impressão digital é a mesma do ledger, nada é gravado, nem um `commit`. Se mudou, a janela é trocada em um `commit` atômico. Uma janela que ficou vazia na fonte perde as linhas.
4. Uma janela que nunca foi carregada com a mesma config é sempre lida. `lookback` limita só as releituras das outras: `lookback: 2` relê as duas últimas janelas.
5. A config é o que a pipeline lê, os `filters`, `select`, `columns` e a coluna e a largura de `batches`. Uma config nova faz o ledger tratar todas as janelas como não carregadas. O intervalo (`begin`, `end`) e `lookback` não entram, porque só escolhem quais janelas rodam.
6. `append` nunca regrava o que não mudou. Uma mudança de colunas falha com `SchemaMismatchError`. `overwrite` regrava todas as janelas, apaga as janelas fora do intervalo e aceita um esquema novo.
7. Depois de gravar, o fluxo escreve um `checkpoint` (o BigQuery lê o log) e os arquivos trocados ficam sete dias antes do `vacuum`.

Tudo o que vai de uma fonte para o bucket e vira uma tabela BigLake usa `Delta`: `dump_db_to_gcs`, `dump_arcgis_to_gcs`, `dump_gsheet_to_gcs` e `upload_files_to_gcs`. Do BigQuery para o bucket é um `export`, então `dump_bq_to_gcs` usa só lotes simples de `Parquet`, sem `Delta`.

**O que ainda custa**

- Uma janela que não mudou ainda é lida na fonte, porque a impressão digital precisa das linhas. Use `lookback` para ler só as janelas que podem mudar, e rode uma passada completa (sem `lookback`) de vez em quando para pegar mudanças nas janelas antigas.
- Uma consulta nativa (`read.query`) roda inteira na fonte, e o DuckDB não consegue empurrar a janela para dentro dela. Por isso ela é uma janela só. O Oracle é a exceção: ele mesmo aplica os filtros e as janelas, numa tabela ou numa consulta.
- Uma consulta nativa roda duas vezes: uma para descrever as colunas e outra para extrair. Uma consulta lenta custa o dobro.
- O BigQuery lê a tabela `Delta` por uma conexão BigLake (`cloud.biglake_connection`), com os limites da Google: leitor versão 3 no máximo, sem `Storage Read API`.
- O `deltalake` lê o ADC pelo `object_store` do Rust. Isso funciona com o servidor de metadados do GKE (workload identity), com um arquivo de service account e com credenciais de usuário. **Não funciona** com credencial `external_account` (federação). Confira a credencial do pod antes de ligar a pipeline.

**Tipos e dialetos** (conferidos em Postgres, MySQL, SQL Server e Oracle de verdade)

- O `Delta` não guarda `TIME`, `UUID` nem `INTERVAL`: eles chegam como texto. Um inteiro sem sinal chega no inteiro com sinal maior (`UINTEGER` em `BIGINT`) e `UBIGINT` chega em `DECIMAL(20, 0)`.
- Um `timestamp` com fuso (`timestamptz`, `datetimeoffset`) é cortado em dias de São Paulo, em qualquer máquina. Um `timestamp` sem fuso vale como vem do banco, e o `TIMESTAMP` do MySQL vem em UTC.
- `normalize_values` limpa o texto de todo banco, inclusive o `nvarchar` do SQL Server.
- **SQL Server:** o `DuckDB` corta toda consulta em 30 s por padrão. O toolkit troca esse limite por `DB_QUERY_TIMEOUT_SECONDS` (3600). Numa consulta nativa, uma coluna `varchar` com acento falha com `Invalid unicode`: use `CAST(coluna AS NVARCHAR(n))` ou leia pela tabela (`read.table`), que decodifica certo.
- **Oracle:** é lido com o `oracledb` e o Instant Client (modo `thick`, que a imagem base já tem; sem ele o `oracledb` usa o modo `thin`, que não lê usuários com verifier `10G`). A pasta do Instant Client vem de `ORACLE_CLIENT_LIB_DIR` ou do `ldconfig`. Cada janela vira um `Parquet` local, com os filtros e a janela aplicados pelo Oracle por variáveis de bind: `read.table` é `DONO.TABELA` e `read.query` é uma consulta, que pode ter `;` no fim, e as duas aceitam `batches` e `filters`. Os nomes das colunas são os que o Oracle informa, em maiúsculas, como em `batches: { column: DATA_PEDIDO }`. A tabela é lida `AS OF SCN`, com o mesmo SCN em todas as janelas, e uma mudança feita durante a carga não entra. Isso pede `EXECUTE` em `SYS.DBMS_FLASHBACK` ou `SELECT` em `SYS.V_$DATABASE`, e o privilégio `FLASHBACK` na tabela de outro dono; sem eles a carga segue sem o `SCN` e avisa no log. Uma consulta não é lida `AS OF SCN`. A sessão usa o fuso de São Paulo.
- **Tipos do Oracle:** um `NUMBER` nunca perde dígitos: até 18 dígitos sem escala é `int64`, com precisão e escala que cabem é `decimal128`, `FLOAT` é `double`, e um `NUMBER` sem precisão (ou que o `decimal128` não guarda) chega como texto exato. `DATE` e `TIMESTAMP` chegam sem fuso (`DATE` tem hora). `TIMESTAMP WITH TIME ZONE` e `WITH LOCAL TIME ZONE` chegam como o instante em UTC. `CLOB`, `NCLOB`, `NVARCHAR2` e `XMLTYPE` chegam como texto, `RAW` e `BLOB` como bytes, e `INTERVAL` e `ROWID` como texto. Um tipo sem conversão (objeto, `BFILE`, `BOOLEAN`, `JSON`) falha e diz a coluna: converta na consulta. O `Oracle` 18c (XE), 19c (19.9, `WE8MSWIN1252`) e 23 passaram nos mesmos testes.
- **Postgres e MySQL:** o servidor parado no meio de uma leitura deixa a execução esperando para sempre. Ponha `timeout_seconds` na tarefa, no `Prefect`.

**Testes do dump: o que cada nível usa de verdade** (nenhum nível usa contêiner)

| Nível  | Como rodar                                                                              | O que é real                                                                                                                                                                                                                              | O que é simulado                                                                                        |
| ------ | --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Padrão | `pytest`                                                                                | `deltalake` e `DuckDB` de verdade, em uma pasta local. Um leitor que não é o nosso (a extensão `delta` do `DuckDB`) lê a tabela depois de cada tipo de escrita, e o log é conferido contra os limites que a Google lista para o BigQuery. | O GCS é uma pasta local, o BigQuery é o leitor `DuckDB`, e o bucket das exportações é um `MemoryStore`. |
| `gcp`  | `TOOLKIT_GCP_PROJECT=meu-projeto GOOGLE_APPLICATION_CREDENTIALS=adc.json pytest -m gcp` | GCS, BigQuery e BigLake de verdade.                                                                                                                                                                                                       | A fonte é uma pasta de arquivos `Parquet`.                                                              |

O nível `gcp` precisa de `gcloud`, `bq` e do ADC com permissão para criar um bucket, uma conexão BigLake, um dataset e uma service account no projeto. Ele cria tudo com um sufixo aleatório e apaga tudo no fim, mesmo se um teste falhar. Cada teste guarda um achado dos testes reais:

| Achado                                                                                                                                                                                    | Onde o teste está                                                     |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| O BigQuery lê a tabela `Delta` que o `deltalake` grava no `gs://`, com as linhas da fonte.                                                                                                | `gcp`                                                                 |
| A mesma carga de novo não faz `commit` nem objeto novo. Uma janela trocada não duplica linha, e uma janela vazia perde as linhas.                                                         | Padrão e `gcp`                                                        |
| Um `overwrite` com outras colunas só aparece no BigQuery depois de `publish_table_in_bq` atualizar a tabela, e um segundo `publish` atualiza em vez de falhar.                            | Padrão e `gcp`                                                        |
| `timestamp` sem fuso é `timestamp_ntz` no `Delta` e `DATETIME` no BigQuery, com fuso é `TIMESTAMP`, e `decimal` é `NUMERIC`. O protocolo fica na versão 3 do leitor, sem `checkpoint` V2. | Padrão (o tipo do `Delta` e o protocolo) e `gcp` (o tipo do BigQuery) |
| Quatro execuções ao mesmo tempo não perdem janela no `gs://` nativo. Pela API S3 do GCS com chaves HMAC elas perdem `commits`, por isso o `deltalake` nunca recebe `storage_options`.     | `gcp` (a corrida) e padrão (a guarda)                                 |
| O `checkpoint` existe depois do dump, e o BigQuery continua lendo.                                                                                                                        | Padrão e `gcp`                                                        |
| O primeiro `overwrite` apaga os arquivos `Hive` antigos, e os seguintes nunca apagam a tabela `Delta`.                                                                                    | Padrão e `gcp`                                                        |
| Do BigQuery para o bucket sai um arquivo `Parquet` por lote.                                                                                                                              | Padrão e `gcp`                                                        |
| Uma coluna de lote com o tipo errado falha antes de qualquer escrita. Um MySQL de verdade devolveu uma data como `VARCHAR` e o banco recusou a comparação com uma data.                   | Padrão                                                                |

### Pipelines

Cada pipeline possui:

- `Dockerfile`: imagem customizada para execução no Prefect
- `flow.py`: definição dos fluxos Prefect
- `prefect.yaml`: configuração do deployment Prefect
- `pyproject.toml`: dependências específicas da pipeline

Alterações específicas de cada pipeline devem ser adicionadas no `Dockerfile` de cada pipeline, assim como dependências específicas devem ser adicionadas no `pyproject.toml` de cada pipeline.

O arquivo `prefect.yaml` é responsável por definir as configurações de deployment do flow Prefect. Esse arquivo especifica variáveis de ambiente, parâmetros, agendamento, infraestrutura de execução e outras opções necessárias para o correto funcionamento do fluxo. O arquivo também é utilizado nos workflows de CI/CD para registrar e disponibilizar os flows.

Consulte a [documentação oficial do Prefect](https://docs.prefect.io/v3/how-to-guides/deployments/prefect-yaml) para detalhes sobre todas as opções disponíveis.

#### Configuração de secrets

O parâmetro `secretName` nas `job_variables` do `prefect.yaml` especifica qual Kubernetes Secret deve ser montado no container durante a execução do job. Esse secret contém variáveis de ambiente necessárias para o funcionamento da pipeline, como credenciais de banco de dados, tokens de API e outras informações sensíveis.

**Valores padrão utilizados:**

- **`prefect-jobs-secrets`**: para deployments de produção
- **`prefect-jobs-secrets-staging`**: para deployments de staging

Exemplo de configuração no `prefect.yaml`:

```yaml
deployments:
  - name: rj-secretaria--pipeline--prod
    work_pool:
      job_variables:
        secretName: prefect-jobs-secrets
  - name: rj-secretaria--pipeline--staging
    work_pool:
      job_variables:
        secretName: prefect-jobs-secrets-staging
```

### Convenções de nomenclatura

- Os diretórios de pipelines seguem o padrão: `rj_<secretaria>__<pipeline>`, utilizando apenas letras minúsculas, números e underscores.
- Os arquivos principais de cada pipeline (`Dockerfile`, `flow.py`, `prefect.yaml`, `pyproject.toml`) devem estar diretamente dentro do diretório da pipeline.
- Os nomes de deployments Prefect seguem o padrão `rj-<secretaria>--<pipeline>--<ambiente>`, por exemplo: `rj-segovi--dump-db-1746--staging`.
- Use sempre nomes descritivos e padronizados para facilitar a identificação e automação dos fluxos.

### Templates

Este repositório utiliza [`cookiecutter`](https://cookiecutter.readthedocs.io/) para facilitar a criação de novas pipelines de forma padronizada. Os templates disponíveis em `templates/` permitem gerar rapidamente a estrutura de diretórios e arquivos necessários para uma nova pipeline Prefect, incluindo `Dockerfile`, `flow.py`, `prefect.yaml` e `pyproject.toml`.

Para criar uma nova pipeline, instale `uv` e rode:

```sh
uvx cookiecutter templates --output-dir=pipelines
```

O template cria uma pipeline de dump, pronta para trocar os valores de exemplo: `flow.py` (duas linhas, com o fluxo `dump_database`), `prefect.yaml` (staging e produção, com um `schedule` `overwrite` e um incremental), `pyproject.toml` e `Dockerfile`. Veja o [Início rápido](STYLEGUIDE.md#início-rápido) do guia de estilo.

Você será solicitado a informar valores como `secretaria` e `pipeline`, que serão utilizados para preencher os nomes dos diretórios, arquivos e variáveis nos templates. O template gerado já segue o padrão de nomenclatura definido anteriormente.

### Work pools

Este repositório utiliza dois work pools principais para execução dos deployments Prefect:

- **default-pool**: destinado à execução geral de pipelines, incluindo fluxos que não possuem requisitos especiais de rede ou infraestrutura. É o pool padrão para a maioria dos deployments.
- **onprem-pool**: utilizado para pipelines que acessam bancos de dados ou sistemas internos da IplanRio, especialmente aqueles que exigem conexão via VPN. Esse pool garante que os jobs sejam executados em ambientes com acesso seguro e autorizado aos recursos internos.

Ao configurar um deployment no arquivo `prefect.yaml`, selecione o pool apropriado conforme a necessidade de acesso aos dados ou sistemas.

## CI/CD

O repositório utiliza GitHub Actions para automatizar todo o ciclo de vida das pipelines, incluindo build, deploy e publicação de imagens Docker:

- **Deploy automático dos flows**: os workflows `.github/workflows/deploy-prefect-flows-prod.yaml` e `.github/workflows/deploy-prefect-flows-staging.yaml` realizam o deploy automático dos flows Prefect para os ambientes de produção e staging, respectivamente.
  - O workflow de produção é acionado a cada push na branch `master` ou manualmente, sempre que houver alterações em arquivos dentro de `pipelines/**`.
  - O workflow de staging é acionado a cada push em branches `staging/*` ou manualmente, também monitorando alterações em `pipelines/**`.
  - Ambos executam:
    - Checkout do código-fonte
    - Login no GitHub Container Registry (`ghcr.io`)
    - Instalação das dependências Python com `uv`
    - Execução do script `.github/scripts/deploy_prefect_flows.py`, que faz o deploy automático de todos os flows definidos em `pipelines/*/prefect.yaml`
    - Caso algum deploy falhe, o workflow é interrompido e o erro é registrado nos logs

- **Build e publicação da imagem base**: o workflow `.github/workflows/build-and-push-root-dockerfile.yaml` é acionado em alterações no `Dockerfile` da raiz a cada push na branch `master`, além de poder ser executado manualmente. Ele realiza:
  - Build da imagem Docker definida no `Dockerfile` do repositório
  - Publicação da imagem no GitHub Container Registry (`ghcr.io/${{ github.repository }}:latest`)

## Troubleshooting

### Erro: `uv sync --package` falha com "package not found"

**Causa**: O `pyproject.toml` da pipeline não declara `prefect_rj_iplanrio` como dependência com `workspace = true`.

**Solução**: Adicione ao `pyproject.toml` da pipeline:

```toml
dependencies = [
    "prefect_rj_iplanrio",
    # ... outras dependências
]

[tool.uv.sources]
prefect_rj_iplanrio = { workspace = true }
```

### Erro: Docker build falha com "cx-oracle" ou "pkg_resources"

**Causa**: O pacote `cx-oracle` não é compatível com Python 3.13+.

**Solução**: O `pyproject.toml` raiz já contém um override para usar `oracledb` no lugar de `cx-oracle`. Certifique-se de que o `uv.lock` está atualizado:

```sh
uv lock
```

### Erro: Dependências base não instaladas no container

**Causa**: O Dockerfile não está copiando os diretórios `src/` e `packages/`.

**Solução**: Verifique se o Dockerfile da pipeline contém:

```dockerfile
COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/
COPY ./packages ./packages/
COPY ./pipelines/<nome-da-pipeline> ./pipelines/<nome-da-pipeline>/
RUN uv sync --package <nome-da-pipeline>
```

### Erro: Lockfile desatualizado após adicionar nova pipeline

**Causa**: O `uv.lock` precisa ser regenerado quando novas pipelines são adicionadas ao workspace.

**Solução**: Execute na raiz do repositório:

```sh
uv lock
```

### Erro: Import de código compartilhado não funciona

**Causa**: O pacote `prefect_rj_iplanrio` não está sendo instalado corretamente.

**Solução**: Verifique se:
1. O diretório `src/prefect_rj_iplanrio/` existe e contém `__init__.py`
2. A pipeline declara `prefect_rj_iplanrio` como dependência com `workspace = true`
3. O Dockerfile copia os diretórios `src/` e `packages/`
