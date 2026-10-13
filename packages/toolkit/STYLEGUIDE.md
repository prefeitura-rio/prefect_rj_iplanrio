---
lang: pt-br
---

# Guia de Estilo do toolkit

Regras para escrever, estruturar e manter o pacote `toolkit`. O pacote é DuckDB-first: DuckDB executa SQL e faz a E/S de Parquet, Polars cobre o que o DuckDB não faz.

O `toolkit` segue o [`STYLEGUIDE.md`](../../STYLEGUIDE.md) da raiz: estilo Python, dados tabulares, validação, testes e higiene do repositório. Este arquivo tem apenas as regras que só existem no `toolkit`. A base do código é o serviço `data-proxy`; siga um padrão existente antes de criar outro.

## Índice

1. [Filosofia](#1-filosofia)
2. [Critérios de promoção](#2-critérios-de-promoção)
3. [Layout](#3-layout)
4. [API pública e contratos de import](#4-api-pública-e-contratos-de-import)
5. [SQL e DuckDB](#5-sql-e-duckdb)
6. [Engines e fontes](#6-engines-e-fontes)
7. [Google Cloud](#7-google-cloud)
8. [Fronteira com o Prefect](#8-fronteira-com-o-prefect)
9. [Testes do toolkit](#9-testes-do-toolkit)
10. [Verificações automáticas](#10-verificações-automáticas)

## 1. Filosofia

O `toolkit` fornece primitivos reutilizáveis. Ele não contém lógica de domínio. A tabela das três camadas está na [Seção 2 do guia raiz](../../STYLEGUIDE.md#2-arquitetura-de-código-em-três-camadas).

**Regra central:** regras de negócio, transformações específicas de uma secretaria e SQL de uma pipeline pertencem à pipeline. Quando uma função aqui começa a acumular `if dataset_id == "brutos_x":`, ela desceu um nível demais.

## 2. Critérios de promoção

### 2.1 Quando promover

Uma função sobe para o `toolkit` quando **todas** as condições forem verdadeiras:

1. Duas ou mais pipelines precisam dela, e ela é um primitivo genérico.
2. Não contém lógica específica de nenhuma pipeline, secretaria ou dataset.
3. É estável o bastante para ser um contrato público. Renomeá-la depois quebra os consumidores.

Promoção especulativa é proibida. Copie uma vez; promova no segundo uso confirmado.

### 2.2 O que nunca pertence aqui

| Artefato                            | Motivo                                                      |
| ----------------------------------- | ----------------------------------------------------------- |
| Lógica de negócio de uma secretaria | Pertence ao `utils.py` da pipeline.                         |
| Queries de uma pipeline             | Vivem em `queries/*.sql` da pipeline que as usa.            |
| Constantes de dataset ou table ID   | São configuração da pipeline, não primitivos de biblioteca. |
| `@flow`                             | Flows são deployments. Pertencem às pipelines.              |
| Código gerado sem revisão           | Notas de sessão e arquivos temporários não são código.      |

## 3. Layout

### 3.1 Módulos

O pacote é plano. Cada módulo cobre um domínio e leva o nome do domínio.

| Módulo                                                    | Responsabilidade                                                                                                                                                                                                                              |
| --------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `toolkit.executor`                                        | O `Executor`, que roda os templates SQL num `Engine` lendo de um `Source`. `engines/` (protocolo `Engine` e `DuckDBEngine`), `sources/` (protocolo `Source`, fontes fornecidas e `registry.py`), `templates.py`, `constants.py` e `utils.py`. |
| `toolkit.templates/`                                      | Arquivos `.sql` (Jinja), uma pasta por backend: `duckdb/`, `postgres/`, `mysql/`, `mssql/`, `oracle/`.                                                                                                                                        |
| `toolkit.settings`                                        | `Settings` (pydantic-settings) e leitura de credenciais de banco.                                                                                                                                                                             |
| `toolkit.log`                                             | O logger do pacote.                                                                                                                                                                                                                           |
| `toolkit.types`                                           | Aliases de tipo e protocolos compartilhados.                                                                                                                                                                                                  |
| `toolkit.gcs`                                             | Operações no GCS com `aiobotocore`. Único lugar que importa `aiobotocore`.                                                                                                                                                                    |
| `toolkit.bigquery`                                        | Cliente assíncrono do BigQuery (ADC) e tabelas externas.                                                                                                                                                                                      |
| `toolkit.partitions`                                      | Seleções de partição (união etiquetada) e janelas de data.                                                                                                                                                                                    |
| `toolkit.extraction`                                      | Montagem e execução do `COPY` para o bucket de staging.                                                                                                                                                                                       |
| `toolkit.columns`                                         | Normalização de nomes de coluna.                                                                                                                                                                                                              |
| `toolkit.dump`, `toolkit.dump_url`, `toolkit.dump_arcgis` | Fluxos de extração.                                                                                                                                                                                                                           |
| `toolkit.dbt`, `toolkit.prefect`                          | Execução de dbt e helpers do Prefect.                                                                                                                                                                                                         |
| `toolkit.tasks`                                           | Os `@task` finos que envolvem as funções acima.                                                                                                                                                                                               |

### 3.2 Regras de código

- `__init__.py` fica sempre vazio. Importe dos submódulos.
- Não use `_` como prefixo de nome. Uma função não recebe um parâmetro que não usa.
- Use dataclasses para dados simples. Use modelos Pydantic só para validação em fronteiras (requisições, configurações, linhas lidas do banco).
- Modele alternativas fechadas como união etiquetada e trate cada caso com `match`. Use `_` só para conjuntos abertos.
- Fluxos de vários passos são um dataclass de contexto com um método por passo, como `DumpContext`.
- Use parâmetros explícitos e funções com nome. Evite `**kwargs`, lambdas opacas, `Any` e `object`.
- Código bloqueante roda por `asyncer.asyncify`.
- Não dê a um módulo o nome de uma biblioteca padrão ou de terceiros, como `logging.py` ou `polars.py`.
- Cada constante vive no módulo que a usa, exceto as de `toolkit.constants`, que são compartilhadas.

## 4. API pública e contratos de import

### 4.1 Caminhos de import são contratos

O `toolkit` é usado por todas as pipelines sem versionamento semântico. Cada caminho de import é um contrato. Renomear ou mover um símbolo público quebra os consumidores sem aviso.

```python
# Consumidor depende deste import exato
from toolkit.dump import DumpRequest
```

### 4.2 Regras de compatibilidade

| Operação                        | Seguro?    | Ação necessária                                       |
| ------------------------------- | ---------- | ----------------------------------------------------- |
| Adicionar nova função           | ✅ Sim     | Nenhuma.                                              |
| Adicionar parâmetro com default | ✅ Sim     | Documente o parâmetro.                                |
| Remover parâmetro com default   | ⚠️ Cuidado | Verifique todos os consumidores antes.                |
| Renomear função                 | ❌ Não     | Atualize todos os consumidores no mesmo PR.           |
| Mover função entre módulos      | ❌ Não     | Idem, ou mantenha um alias no módulo original.        |
| Alterar tipo de retorno         | ❌ Não     | Quebra os consumidores que dependem do tipo anterior. |

### 4.3 Alias durante uma migração

Ao mover um módulo, o caminho antigo reexporta tudo do novo enquanto os consumidores são atualizados. O alias é dívida: não adicione símbolos novos a ele e remova-o quando o último consumidor mudar.

```python
# toolkit/old_name.py  ← arquivo temporário de migração
# Não adicione símbolos aqui. Atualize os consumidores e remova este arquivo.
from toolkit.new_name import *  # noqa: F401, F403  # alias de migração
```

Este é o único lugar onde `import *` é aceito. Ele é a exceção à regra da Seção 5.3 do guia raiz.

## 5. SQL e DuckDB

### 5.1 Todo SQL passa pelo `Executor`

- O SQL do toolkit vive em `toolkit/templates/<backend>/<nome>.sql`; o da pipeline vive em `queries/*.sql` e também passa pelo `Executor` (Seção 5.4). Nunca há SQL em arquivo Python, exceto a expressão `FROM` que um adaptador `Source` devolve.
- O `Executor` renderiza o template (Jinja, `StrictUndefined`) e o executa num `Engine`. Código fora do `Executor` não abre conexão DuckDB nem escreve SQL.
- Valores entram no template como `Identifier()` e `Literal()` de `psycopg.sql`. Nunca coloque um valor vindo de fora no texto do SQL. Valores de execução usam parâmetros (`?`).
- Cada template começa com um cabeçalho JSON em comentário Jinja que descreve a entrada.
- Uma conexão DuckDB atende uma consulta por vez. Abra uma conexão por tarefa concorrente.

```python
rows = await executor.query(
    "duckdb/describe_source", {"source": scan}, expect=ColumnInfo
)
```

### 5.2 Linhas viram modelos

`Executor.query(..., expect=Modelo)` valida cada linha com Pydantic e levanta `ValidationError` se uma linha não couber no modelo. Use um modelo por consulta. Não leia tuplas soltas.

### 5.3 DuckDB primeiro, Polars no resto

| Ferramenta | Papel                                                                                           |
| ---------- | ----------------------------------------------------------------------------------------------- |
| DuckDB     | SQL, leitura das fontes, escrita de Parquet e CSV, `COPY` para o bucket, geometria (`spatial`). |
| Polars     | Só o que o DuckDB não faz: a leitura de Oracle por lotes e o preparo de dados de APIs.          |

Uma função pública recebe e devolve `Path`, `pl.DataFrame` ou `pl.LazyFrame`. Ela nunca expõe conexão ou relação do DuckDB, nem cursor de driver.

### 5.4 Limpeza de dados

O `Executor` faz toda a limpeza que o DuckDB faz, isto é, que é SQL. O que o DuckDB não faz (lógica em Python, chamadas de API, quadros do Polars) é trabalho da task da pipeline, que orquestra a chamada.

- Um passo de limpeza é um `Step(template, values)`. O template é um `SELECT` que lê `{{ source }}`. `Executor.clean(scan, steps)` aplica os passos em ordem: cada passo vira uma `TEMP VIEW` sobre o anterior, com nome único, e o resultado é a expressão `FROM` da última. Os dados são lidos uma vez, quando o resultado é copiado.
- Uma pipeline passa a pasta dela (`Executor(engine, templates=Path(__file__).parent / "queries")`, ou `templates=` em `run_dump` e `stage_scan`) e uma lista de `Step`. O Executor procura o template primeiro nessa pasta e depois no toolkit, então um template da pipeline pode importar os macros do toolkit (`{% from "duckdb/macros.sql" import text %}`). Não use nomes de pasta do toolkit, como `duckdb/`, na pasta da pipeline.
- Os valores de um `Step` são `Identifier`, `Literal` ou `SQL`, números ou booleanos, nunca texto simples, que entraria cru no SQL. Um `str` levanta `TypeError` com o nome da chave quando o `Step` é criado.
- Um erro num passo carrega uma nota com o número do passo e o nome do template (`cleaning step 2: trim_names`), sem o texto do SQL.
- A pipeline só escreve arquivos SQL e a lista de passos. Ela não abre conexão DuckDB.
- `DUCKDB_MEMORY_LIMIT` e `DUCKDB_TEMP_DIRECTORY` limitam a memória do DuckDB e escolhem a pasta onde ele grava os dados que não cabem. Vazios, valem os padrões do DuckDB.

```python
steps = [Step("trim_names"), Step("drop_duplicates", {"key": Identifier("id")})]
await run_dump(request, steps=steps, templates=Path(__file__).parent / "queries")
```

## 6. Engines e fontes

O `Executor` recebe um `Engine` e um `Source` e junta os dois. A divisão é esta: o `Engine` roda SQL, o `Source` descreve um sistema remoto e nunca roda SQL, e só o `Executor` roda SQL no engine por causa de um `Source`.

### 6.1 Engine

- `Engine` é um protocolo com `execute(sql, params)` e `records(sql, params)`. Ele roda SQL já renderizado.
- O `toolkit` fornece `DuckDBEngine` (`toolkit/executor/engines/duckdb.py`). Qualquer classe que siga o protocolo `Engine` (`toolkit/executor/engines/protocol.py`) também serve. Passe uma fábrica de engine (`open_engine`) para `run_dump`.
- Uma conexão DuckDB atende uma consulta por vez. A fábrica abre um engine por tarefa concorrente.

### 6.2 Source

- `Source` é um protocolo (`toolkit/executor/sources/protocols.py`) que descreve de onde vêm os dados: `name` e `extensions` (extensões do DuckDB a carregar), que são atributos de classe (`ClassVar`), `window(query, selection)`, `setup()` (SQL a rodar antes da leitura, como o `ATTACH`, ou `None`) e `scan(query)` (a expressão `FROM`).
- Uma função não recebe um parâmetro que não usa. Por isso uma fonte recebe tudo de que precisa quando é criada, por um `SourceContext` (dados de conexão, diretório de trabalho e tamanho de lote): cada construtor lê só os campos que usa.
- Uma fonte que o engine não lê direto também segue o protocolo `Fetcher` (`async fetch(query)`), que copia os dados para arquivos locais e devolve a lista deles. `Executor.prepare_source` segue sempre esta ordem: `fetch` (se houver; sem arquivos, não há o que copiar e devolve `None`), carregar `extensions`, rodar `setup` e devolver `scan`. A fonte não tem referência ao engine.
- O `toolkit` fornece cinco fontes, uma classe por arquivo em `toolkit/executor/sources/backends/`: `PostgresSource`, `MySQLSource`, `MSSQLSource`, `OracleSource` e `BigQuerySource`. Todas têm `from_context(context)`, que recebe o `SourceContext`. O método é opcional: `SourceBase` o define como `cls()`, para uma fonte que não precisa do contexto (como `BigQuerySource`). Uma fonte com campos obrigatórios, como a conexão, o sobrescreve. Qualquer outra função ou classe que receba um `SourceContext` e devolva um `Source` também serve: passe-a como `make_source` para `run_dump`.
- Postgres, MySQL e SQL Server herdam de `DuckDBSource` (`toolkit/executor/sources/duckdb.py`), que tem toda a lógica: `setup` cria um `SECRET` e faz `ATTACH`, e `scan` chama a função de consulta da extensão. A subclasse só define atributos de classe, cada um anotado como `ClassVar` (`attach_type`, `query_function` e, se preciso, `extensions` e `read_only`), e não sobrescreve métodos. Se um backend precisar de outro comportamento, sobrescreva só aquele método na classe dele; não crie opções na classe base. O BigQuery declara a extensão `bigquery`, não tem `setup` e `scan` devolve `bigquery_query(projeto, sql)`, sem dados de conexão. As fontes que precisam da conexão usam `context.require_conn(nome)`.
- Oracle não tem extensão no DuckDB. `fetch` lê por lotes com `oracledb` e Polars e escreve Parquet no diretório de trabalho, e `scan` devolve `read_parquet` sobre esses arquivos. O DuckDB faz o `COPY` para o bucket, como nas outras fontes.
- O registro usa a biblioteca `autoregistry`. `SourceBase` (`toolkit/executor/sources/base.py`) é a base das fontes fornecidas: uma subclasse se registra pelo nome da classe sem o sufixo `Source`, em minúsculas (`PostgresSource` é `"postgres"`), e o nome da classe precisa terminar em `Source`. O atributo `name` da classe vem desse nome, então a subclasse não o declara. Uma classe intermediária, como `DuckDBSource`, usa `skip=True` e não entra no registro. `load_sources()` (`toolkit/executor/sources/registry.py`) importa todos os módulos de `backends/` e devolve o registro, usado como um dicionário: `load_sources()["postgres"]` é a classe, e a busca ignora maiúsculas. Um arquivo novo em `backends/` já basta. Dois nomes iguais levantam `KeyCollisionError`. Uma fonte fora de `backends/` é passada como `make_source` (uma classe que só segue o protocolo `Source` funciona sem registro).
- A janela de datas é aplicada na consulta remota, no dialeto da fonte, por um template `window.sql` por fonte. O nome da coluna passa por `safe_column` (`toolkit/executor/utils.py`).
- A conexão (`DBConn`, em `protocols.py`) é um `dataclass(frozen=True)` com a senha fora do `repr`. Nunca registre senha, token, string de conexão, valores ou texto de consulta em log.
- A construção de um `Source` não tem efeito colateral. Clientes externos são criados no primeiro uso.

## 7. Google Cloud

- **Autenticação:** workload identity (WIF) por Application Default Credentials. Não existe arquivo de service account, variável `GOOGLE_APPLICATION_CREDENTIALS` nem credencial em imagem. Clientes Google usam `google.auth.default(scopes=...)`.
- **GCS:** apenas por `toolkit.gcs` (`aiobotocore`, API compatível com S3, chaves HMAC lidas de `Settings`). O GCS não tem exclusão em lote do S3: apague objeto por objeto, com concorrência limitada.
- **Escrita no bucket:** o DuckDB escreve com `COPY ... TO 'gs://...'`, usando o `SECRET` do tipo `gcs` criado pela conexão.
- **BigQuery:** apenas por `toolkit.bigquery`. A tabela final é uma tabela externa sobre o prefixo de staging, criada depois que os dados existem.

## 8. Fronteira com o Prefect

Cada função pura vive no módulo do seu domínio. O `@task` que a envolve vive em `toolkit/tasks.py` e é um wrapper fino.

```python
# toolkit/dump.py
async def run_dump(request: DumpRequest) -> None:
    """Dump a database query to the staging bucket."""
    ...


# toolkit/tasks.py
@task
async def dump_database_task(request: DumpRequest) -> None:
    """Dump a database query to the staging bucket and publish the table."""
    await run_dump(request)
```

| Padrão                                | Motivo                                                            |
| ------------------------------------- | ----------------------------------------------------------------- |
| `@flow` em qualquer arquivo do pacote | Um flow aqui tem parâmetros fixos e vira uma pipeline disfarçada. |
| Lógica de negócio dentro do `@task`   | A lógica presa ao wrapper não pode ser testada sem o Prefect.     |
| `@task` fora de `toolkit/tasks.py`    | Dificulta achar onde estão as tasks.                              |
| `if __name__ == "__main__":`          | Scripts avulsos não pertencem a uma biblioteca.                   |
| `print()`                             | Use `toolkit.log.logger`.                                         |

## 9. Testes do toolkit

A pasta `packages/toolkit/tests/` segue os níveis do `data-proxy`:

| Nível      | Local                | Roda contra                                                  |
| ---------- | -------------------- | ------------------------------------------------------------ |
| Unitário   | `tests/unit/`        | Um processo: DuckDB em memória, arquivos temporários, fakes. |
| Integração | `tests/integration/` | Serviços locais: servidor S3 `moto`.                         |

As regras gerais de teste estão na [Seção 14 do guia raiz](../../STYLEGUIDE.md#14-testes). Regras só do toolkit:

- Testes unitários não abrem rede. Testes que instalam uma extensão do DuckDB levam a marca `extensions`.
- O arquivo de teste espelha o módulo: `toolkit/partitions.py` é testado por `tests/unit/test_partitions.py`.
- Use DuckDB em memória e `tmp_path`. Não use fake de DuckDB: ele é rápido o bastante para ser real.
- Mocke só serviços externos (BigQuery, GCS real, bancos). Cada fake tem a forma mínima do que o código usa.
- Use `hypothesis` para invariantes, como "as janelas cobrem o intervalo sem lacuna nem sobreposição".
- Todo bug achado ganha um teste de regressão com o cenário no `id`.

## 10. Verificações automáticas

As regras acima são verificadas por ferramentas. Rode no diretório `packages/toolkit/`:

```sh
uv run ruff check . && uv run ruff format --check .
uv run basedpyright
uv run sqlfluff lint toolkit/templates
uv run vulture toolkit
uv run pytest
```

| Regra                                                                                                       | Ferramenta                             |
| ----------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| Sem `pandas`, `geopandas`, `basedosdados`, `google.cloud.storage`, `boto3`, `psycopg2`, `pymysql`, `pyodbc` | `ruff` (`TID251`)                      |
| `aiobotocore` só em `toolkit/gcs.py`                                                                        | `ruff` (`TID251`, exceção por arquivo) |
| Sem `print`                                                                                                 | `ruff` (`T20`)                         |
| Tipos completos                                                                                             | `basedpyright`                         |
| Estilo dos templates SQL                                                                                    | `sqlfluff`                             |
| Código sem uso                                                                                              | `vulture`                              |

O hook de pre-commit executa `ruff-check` e `ruff-format` em todo commit que muda arquivos do `packages/toolkit/`.
