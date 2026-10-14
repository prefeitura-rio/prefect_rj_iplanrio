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

O `toolkit` fornece primitivos reutilizáveis. Ele não contém lógica de domínio. A tabela das três camadas está na [Seção 2 do guia raiz](../../STYLEGUIDE.md#2-arquitetura-de-código-em-três-camadas), e os princípios gerais (ler antes de escrever, apagar antes de abstrair, causa antes de sintoma, uma fonte de verdade) estão na [Seção 1.1](../../STYLEGUIDE.md#11-princípios). As regras de commit e de documentação estão nas Seções 12.3 e 12.4 do mesmo guia.

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

| Módulo                               | Responsabilidade                                                                                                                                                                                                                                                                                                                                                                           |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `toolkit.executor`                   | O `Executor`, que roda os templates SQL num `Engine` lendo de um `Source`. `engines/` (protocolo `Engine` e `DuckDBEngine`), `sources/` (protocolo `Source`, fontes fornecidas e `registry.py`), `render.py`, `constants.py`, `types.py`, `utils.py` e `templates/` (arquivos `.sql` em Jinja, uma pasta por backend: `duckdb/`, `postgres/`, `mysql/`, `mssql/`, `oracle/`, `bigquery/`). |
| `toolkit.settings`                   | `Settings` (pydantic-settings) e leitura de credenciais de banco.                                                                                                                                                                                                                                                                                                                          |
| `toolkit.telemetry`                  | O logger do pacote (`prefect.toolkit`) e os logs, traces e métricas OpenTelemetry                                                                                                                                                                                                                                                                                                          |
| `toolkit.constants`, `toolkit.types` | As constantes e os aliases de tipo compartilhados. `toolkit/types.py` não importa nada do toolkit.                                                                                                                                                                                                                                                                                         |
| `toolkit.gcs`                        | Operações no GCS com `obstore`. Único lugar que importa `obstore`.                                                                                                                                                                                                                                                                                                                         |
| `toolkit.bigquery`                   | Cliente assíncrono do BigQuery (ADC) e tabelas externas.                                                                                                                                                                                                                                                                                                                                   |
| `toolkit.dbt`                        | Execução de dbt.                                                                                                                                                                                                                                                                                                                                                                           |

### 3.2 Regras de código

- `__init__.py` fica sempre vazio. Importe dos submódulos.
- Não use `_` como prefixo de nome. Uma função não recebe um parâmetro que não usa.
- Use dataclasses para dados simples. Use modelos Pydantic só para validação em fronteiras (requisições e configurações).
- Modele alternativas fechadas como união etiquetada e trate cada caso com `match`. Use `_` só para conjuntos abertos.
- Fluxos de vários passos são um dataclass de contexto com um método por passo.
- Incorpore um helper de uma linha que tem um só chamador.
- Ponha uma função no módulo do seu domínio. Use `utils.py` só quando nenhum domínio serve.
- Use parâmetros explícitos e funções com nome. Evite `**kwargs`, lambdas opacas, `Any` e `object`.
- Código bloqueante roda por `asyncio.to_thread`.
- Não dê a um módulo o nome de uma biblioteca padrão ou de terceiros, como `logging.py` ou `polars.py`.
- Constantes ficam em `constants.py` e aliases de tipo em `types.py`, nunca soltos nos módulos. Cada pacote tem os seus: `toolkit/constants.py` e `toolkit/executor/constants.py`, `toolkit/types.py`. Um módulo importa do próprio pacote e dos pacotes acima, nunca dos abaixo. `toolkit/types.py` não importa nada do toolkit; um alias que nomeia uma classe fica junto da classe, para não criar ciclo de import. Classes (modelos, dataclasses, protocolos) ficam com o comportamento delas.

## 4. API pública e contratos de import

### 4.1 Caminhos de import são contratos

O `toolkit` é usado por todas as pipelines sem versionamento semântico. Cada caminho de import é um contrato. Renomear ou mover um símbolo público quebra os consumidores sem aviso.

```python
# Consumidor depende deste import exato
from toolkit.dbt import DbtRun
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

- O SQL do toolkit vive em `toolkit/executor/templates/<pasta>/<nome>.sql`; o da pipeline vive em `queries/*.sql` e também passa pelo `Executor`. Não há SQL em arquivo Python.
- O `Executor` renderiza o template (Jinja, `StrictUndefined`) e o executa num `Engine`. Código fora do `Executor` não abre conexão DuckDB nem escreve SQL.
- Valores entram no template como `Identifier()` e `Literal()` de `psycopg.sql`. Nunca coloque um valor vindo de fora no texto do SQL. Valores de execução usam parâmetros (`?`).
- Cada template começa com um cabeçalho JSON em comentário Jinja que descreve a entrada.
- Uma conexão DuckDB atende uma consulta por vez. Abra uma conexão por tarefa concorrente.

```python
orders = await executor.query("orders", {"source": scan}, expect=Order)
```

### 5.2 Resultados viram datasets tipados

`Executor.query(..., expect=Schema)` devolve um `Dataset[Schema]` do Polars (`dataset.py` guarda o resultado e `schema.py` lê a classe e checa os lotes, ambos em `toolkit/executor/`). O schema é uma classe com anotações (`TypedDict`, dataclass ou `NamedTuple`), e `X | None` marca uma coluna que aceita nulos. O resultado se pode `await` (tudo de uma vez) ou percorrer com `async for` (um `Dataset` por lote de `batch_size` linhas).

- Cada lote é checado de forma vetorizada: nomes de coluna iguais aos do schema, tipo da mesma família da anotação (inteiros viram `Int64`, floats viram `Float64`, decimal e timestamp mantêm a precisão), nenhum nulo em coluna sem `| None`. Um `SchemaError` lista todos os problemas.
- Uma coluna `GEOMETRY` do DuckDB usa a anotação `Geometry` (`toolkit.types`) e chega como vem: o tipo de extensão `geoarrow.wkb` do Polars, com o CRS nos metadados. Não converta no template só para ler o dado.
- Um texto nunca é lido como número: quem quiser converter faz o `CAST` no template.
- Regras de campo, como `gt` e `Literal`, não são checadas. Use um modelo Pydantic só onde essa regra importar.
- Uma anotação sem tipo no Polars levanta `TypeError` antes de rodar a consulta. Não leia tuplas soltas.

### 5.3 DuckDB primeiro, Polars no resto

| Ferramenta | Papel                                                                                           |
| ---------- | ----------------------------------------------------------------------------------------------- |
| DuckDB     | SQL, leitura das fontes, escrita de Parquet e CSV, `COPY` para o bucket, geometria (`spatial`). |
| Polars     | Só o que o DuckDB não faz: o preparo de dados de APIs.                                          |

Uma função pública recebe e devolve `Path`, `pl.DataFrame` ou `pl.LazyFrame`. Ela nunca expõe conexão ou relação do DuckDB, nem cursor de driver.

## 6. Engines e fontes

O `Executor` recebe um cliente, e uma fonte é o que abre esse cliente. A divisão é esta: o cliente roda SQL, a `Source` abre e prepara o cliente, e o `Executor` renderiza os templates e os roda no cliente. Uma `Source` nunca monta nem roda uma consulta.

### 6.1 Cliente e Executor

- O cliente segue o protocolo `Engine` (`toolkit/executor/engines/protocol.py`): `execute(sql, params)` e `query(sql, params, batch_size)`, que devolve quadros do Polars em lotes. Um resultado vazio devolve um quadro com as colunas, e um cliente que não sabe as colunas de um resultado vazio devolve um quadro sem colunas. Leia os lotes antes de rodar outro statement no mesmo cliente.
- O `toolkit` fornece `DuckDBEngine` (`toolkit/executor/engines/duckdb.py`). Qualquer classe que siga o protocolo também serve.
- `Executor(client, templates)` só tem `execute` e `query`. `query` devolve um `Dataset` tipado (Seção 5.2). A pasta `templates` da pipeline é procurada antes da do toolkit, então não use nomes de pasta do toolkit, como `duckdb/`, nela.
- Uma conexão DuckDB atende uma consulta por vez. Abra uma conexão por tarefa concorrente.

### 6.2 Source

- `Source` é um protocolo (`toolkit/executor/sources/protocols.py`) com `name` (atributo de classe), `open(engine)` e `setup(client)`.
- `open(engine)` é um gerenciador de contexto assíncrono que entrega o cliente que roda SQL naquele sistema, e fecha o cliente ao sair. Um sistema que o DuckDB anexa (Postgres, MySQL, SQL Server, BigQuery) carrega as extensões e entrega o próprio `engine` do DuckDB. Um que o DuckDB não anexa (Oracle) ignora o `engine` e entrega um cliente próprio (`OracleClient`, sobre `oracledb`). Todo cliente segue o protocolo `Engine`.
- `setup(client)` roda no cliente os statements que o preparam, como o `SECRET` e o `ATTACH`. Quem abre o cliente chama o `setup` em seguida. O padrão de `SourceBase` não faz nada.
- Uma fonte recebe tudo de que precisa quando é criada. As que precisam da conexão têm `from_context(context)`, que lê o `SourceContext` (hoje só os dados de conexão) e usa `context.require_conn(nome)`. Uma fonte sem configuração, como `BigQuerySource`, é criada direto.
- O SQL de um template segue o dialeto do cliente onde roda: DuckDB nas fontes que ele anexa, e Oracle no Oracle. O banco anexado sempre se chama `src` (`ALIAS` em `toolkit/executor/constants.py`), e o template o escreve como `FROM src.esquema.tabela`. Um `Engine` anexa uma fonte só.
- O `toolkit` fornece sete fontes, uma classe por arquivo em `toolkit/executor/sources/backends/`: `PostgresSource`, `MySQLSource`, `MSSQLSource`, `OracleSource`, `BigQuerySource`, `GSheetsSource` e `ArcGISSource`. O `ArcGISSource` só carrega a extensão `spatial`, e o template lê a camada com a macro `arcgis_layer`, sem código HTTP no toolkit. O `GSheetsSource` cria um segredo da extensão `gsheets` com um token de acesso do ADC, e o template chama `read_gsheet` ou `COPY ... (FORMAT gsheet)`. O `BigQuerySource(project)` anexa o projeto somente leitura, com as credenciais do ambiente (ADC), e o projeto padrão é `BIGQUERY_PROJECT`.
- Postgres, MySQL e SQL Server herdam de `DuckDBSource` (`toolkit/executor/sources/duckdb.py`), que tem toda a lógica: `setup` cria um `SECRET` e faz `ATTACH`; a senha vai em `params` e nunca entra no texto do SQL, porque os erros do DuckDB mostram o texto. A subclasse só define atributos de classe, cada um anotado como `ClassVar` (`attach_type` e, se preciso, `extensions` e `read_only`), e não sobrescreve métodos. Se um backend precisar de outro comportamento, sobrescreva só aquele método na classe dele; não crie opções na classe base.
- O registro usa a biblioteca `autoregistry`. `SourceBase` (`toolkit/executor/sources/base.py`) é a base das fontes fornecidas: uma subclasse se registra pelo nome da classe sem o sufixo `Source`, em minúsculas (`PostgresSource` é `"postgres"`), e o nome da classe precisa terminar em `Source`. O atributo `name` da classe vem desse nome, então a subclasse não o declara. Uma classe intermediária, como `DuckDBSource`, usa `skip=True` e não entra no registro. `load_sources()` (`toolkit/executor/sources/registry.py`) importa cada módulo de `backends/` e devolve o registro. Um backend novo é um arquivo novo em `backends/`.
- A conexão (`DBConn`, em `protocols.py`) é um `dataclass(frozen=True)` com a senha fora do `repr`. Nunca registre senha, token, string de conexão, valores ou texto de consulta em log.
- A construção de uma `Source` não tem efeito colateral. Clientes externos são abertos no `open`.

## 7. Google Cloud

- **Autenticação:** workload identity (WIF) por Application Default Credentials. Não existe arquivo de service account, variável `GOOGLE_APPLICATION_CREDENTIALS` nem credencial em imagem. Clientes Google usam `google.auth.default(scopes=...)`.
- **HTTP:** clientes HTTP usam `httpx2`, e `requests` é proibido (`TID251`). O `toolkit` só faz HTTP para renovar o token do Google (`HttpxRequest`, em `gsheets.py`): o DuckDB lê as camadas do ArcGIS e as planilhas.
- **GCS:** apenas por `toolkit.gcs` (`obstore`, API compatível com S3, chaves HMAC lidas de `Settings`; cada função recebe o `ObjectStore` de um bucket, criado por `create_store`). O GCS não tem exclusão em lote do S3: apague objeto por objeto, com concorrência limitada.
- **Escrita no bucket:** o DuckDB escreve com `COPY ... TO 'gs://...'`, usando o `SECRET` do tipo `gcs` criado pela conexão.
- **BigQuery:** apenas por `toolkit.bigquery`. A tabela final é uma tabela externa sobre o prefixo da tabela no bucket, criada depois que os dados existem.

## 8. Fronteira com o Prefect

O toolkit expõe funções puras, sem `@task`. O `@task` que envolve uma função fica no `tasks.py` da pipeline, como wrapper fino (a pipeline decide retries, cache e nome da task).

```python
# toolkit/dbt.py
def execute_dbt(request: DbtRun) -> None:
    """Run one dbt command."""
    ...


# pipelines/<pipeline>/tasks.py
@task(retries=2)
def execute_dbt_task(request: DbtRun) -> None:
    """Run one dbt command."""
    execute_dbt(request)
```

| Padrão                                | Motivo                                                            |
| ------------------------------------- | ----------------------------------------------------------------- |
| `@flow` em qualquer arquivo do pacote | Um flow aqui tem parâmetros fixos e vira uma pipeline disfarçada. |
| Lógica de negócio dentro do `@task`   | A lógica presa ao wrapper não pode ser testada sem o Prefect.     |
| `@task` em qualquer arquivo do pacote | A pipeline decide retries e cache, então o wrapper é dela.        |
| `if __name__ == "__main__":`          | Scripts avulsos não pertencem a uma biblioteca.                   |
| `print()`                             | Use `toolkit.telemetry.logger`.                                   |

### 8.1 Logs, traces e métricas

- Todo log do pacote passa por `toolkit.telemetry.logger`, o logger do Prefect `prefect.toolkit`, com a formatação padrão do Prefect. Escreva `logger.info("Files uploaded: prefix=%s files=%d", prefix, files)`: a mensagem é uma frase de ciclo de vida, `<Componente> <ação> <estado>: contexto`, e o nível segue o guia raiz (Seção 5.5). Não crie logger próprio nem use `print()`.
- Uma operação do pacote (consulta, statement, upload, apagar) é um span: `with tracer.start_as_current_span(NOME, attributes=...)`, com o nome em `constants.py`. O span dá a duração, o erro e o lugar dentro do trace da run. Não crie um histograma por operação.
- O nível do log vem só de `Settings.LOG_LEVEL`. Nenhum módulo define nível. A saída de ferramentas externas, como a do dbt e a do DuckDB, fica como vem.
- Não há métrica de duração: o backend (SigNoz) calcula duração e taxa de erro a partir dos spans. Runs do Prefect não são instrumentadas à mão, porque o Prefect já cria um span por flow run e task run. Não decore flows nem tasks para medir duração.
- Um instrumento de `toolkit/telemetry.py` é só para totais que um span não dá, como linhas e bytes. Use unidade e atributos de cardinalidade baixa (`template`, `prefix`). Valor de dado, id de linha e mensagem de erro não são atributos.
- O pacote usa só a API do OpenTelemetry (`trace.get_tracer`, `metrics.get_meter`). Quem inicia o processo chama `configure_telemetry()` depois de `import prefect`. O endpoint, os cabeçalhos e o nome do serviço são as settings `OTEL_*`, lidas do singleton `settings`; a função não recebe settings como parâmetro.

## 9. Testes do toolkit

Teste o comportamento do nosso código em `packages/toolkit/`, e só dele. Não teste o código dos outros: o DuckDB, o Polars, o Prefect, o OpenTelemetry, o `obstore`, o `psycopg`, o Jinja, o `autoregistry` e os clientes do Google já têm os seus testes. As regras gerais estão na [Seção 14 do guia raiz](../../STYLEGUIDE.md#14-testes). Regras do toolkit:

**Onde ficam**

- Os testes unitários e os de integração moram na mesma árvore, `tests/`, que espelha o pacote: `toolkit/gcs.py` é testado por `tests/test_gcs.py` e `toolkit/executor/schema.py` por `tests/executor/test_schema.py`. Não há pastas `unit/` e `integration/`, nem BDD: são funções `test_*` simples.
- Um teste que precisa de um serviço ou de rede leva a marca do serviço: `s3` (servidor S3) ou `extensions` (instala uma extensão do DuckDB). Os outros testes rodam sem rede.
- Uma fixture usada por mais de um arquivo vai no `conftest.py`. Um módulo de teste só tem testes. Junte arquivos de teste pequenos que cobrem um módulo.

**Como se escreve**

- Um teste é pequeno: prepara, age e confere em poucas linhas. O nome diz o comportamento (`test_a_frame_that_does_not_fit_is_rejected_naming_the_column`), não a função que roda.
- Prefira testes por tabela, `pytest.param(..., id="cenário")`, com o `id` dizendo o cenário. Uma tabela cobre entradas diferentes de um comportamento só.
- Use o `hypothesis` para invariantes, com a frase "para qualquer entrada, isto vale": o limite de concorrência nunca é passado, o inteiro de qualquer largura vira `Int64` sem mudar o valor, a senha nunca entra no texto do SQL. Não use `hypothesis` para um exemplo só.
- Confira valores exatos. Não use `in` sobre texto, salvo uma regex em texto gerado que seria frágil de comparar inteiro.
- Use DuckDB em memória e `tmp_path`. Não faça fake de DuckDB.
- Mocke só o que é externo e não roda aqui: o cliente do Prefect, o runner do dbt, o `oracledb` (um SQLite faz o papel dele) e o token do Google. Cada fake tem a forma mínima do que o código usa.
- Todo bug achado ganha um teste de regressão, com o cenário no `id` e a palavra "Regression" no docstring. Antes de fechar um teste, estrague o código de propósito e veja o teste falhar.

**O que não se testa**

- Logs, spans e métricas. Confira uma vez, à mão, num script em `/tmp`, e não deixe teste no repositório.
- O código do BigQuery (`toolkit/bigquery.py` e `BigQuerySource`): o Google testa o cliente.
- O armazenamento de campos, os valores padrão, os atributos de classe e o "não levantou exceção".
- O conteúdo inteiro de uma lista que cresce, como o registro de fontes: confira só os itens que o teste usa.
- O mesmo comportamento em dois níveis: ponha o teste no nível mais baixo que o prova.

## 10. Verificações automáticas

As regras acima são verificadas por ferramentas. Rode no diretório `packages/toolkit/`:

```sh
uv run ruff check . && uv run ruff format --check .
uv run basedpyright
uv run pyrefly check
uv run sqlfluff lint toolkit/executor/templates
uv run vulture toolkit
uv run pytest
```

| Regra                                                                                                                   | Ferramenta                             |
| ----------------------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| Sem `pandas`, `geopandas`, `requests`, `basedosdados`, `google.cloud.storage`, `boto3`, `psycopg2`, `pymysql`, `pyodbc` | `ruff` (`TID251`)                      |
| `obstore` só em `toolkit/gcs.py`                                                                                        | `ruff` (`TID251`, exceção por arquivo) |
| Sem `print`                                                                                                             | `ruff` (`T20`)                         |
| Tipos completos, no modo `all`                                                                                          | `basedpyright`                         |
| Tipos completos, no preset `strict`                                                                                     | `pyrefly`                              |
| Estilo dos templates SQL                                                                                                | `sqlfluff`                             |
| Código sem uso                                                                                                          | `vulture`                              |

Os dois verificadores de tipos cobrem `toolkit/` e `tests/`, e valem zero erros e zero avisos. A única regra desligada para todo o pacote é `reportUnusedCallResult` do `basedpyright`: chamadas de efeito colateral (BigQuery, dbt, DuckDB) devolvem objetos que ninguém usa. Os testes desligam, por pasta, só as regras que o pytest torna ruído (parâmetros de fixture sem tipo, classes falsas pequenas); cada regra desligada tem o motivo em um comentário no `pyproject.toml`. Não há comentário de ignore no código (`# pyright: ignore`, `# pyrefly: ignore`, `# type: ignore`). Toda supressão vive no `pyproject.toml`, no escopo mais estreito possível (um arquivo ou uma pasta) e com o motivo em um comentário. Para o `basedpyright`, um arquivo é uma `executionEnvironments` com `root` no arquivo; para o `pyrefly`, um `sub-config` com `matches`. O `pyrefly` aplica só o primeiro `sub-config` que casa com o arquivo, então um arquivo listado sozinho repete as regras de `tests/**`. Só uma lacuna de tipos de uma biblioteca de terceiros justifica uma supressão. O hook de pre-commit executa `ruff-check` e `ruff-format` em todo commit que muda arquivos do `packages/toolkit/`.
