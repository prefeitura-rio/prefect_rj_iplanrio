---
lang: pt-br
---

# Guia de Estilo do toolkit

Regras só do pacote `toolkit`. Ele segue o [guia do repositório](../../STYLEGUIDE.md) (Python, dados, testes, repositório) e parte do `data-proxy`: siga um padrão existente antes de criar outro. O comportamento de cada função está na `docstring` dela, não neste guia.

## 1. Escopo

O `toolkit` tem primitivos reutilizáveis e nenhuma lógica de domínio. Regras de negócio, consultas, constantes de `dataset` ou `table_id`, `@flow` e `@task` ficam em cada pipeline ou em `src/utils/`.

Promova uma função para cá só no segundo uso confirmado, e só se ela for genérica, estável e sem lógica de pipeline. Promoção especulativa é proibida.

## 2. Layout e código

| Módulo                               | Responsabilidade                                                                                                                                                                |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `toolkit.executor`                   | `Executor`, `Engine`, fontes e modelos SQL.                                                                                                                                     |
| `toolkit.settings`                   | `ToolkitSettings` (a base) e um grupo por parte, como `GcsSettings`.                                                                                                            |
| `toolkit.functions`                  | As funções do `DuckDB`: `native.py` (uma vez por linha), `arrow.py` e `documents.py` (uma vez por lote) e `registry.py`, com o registro automático.                             |
| `toolkit.utils`                      | As colunas e os filtros de um `dump`: `normalize_column`, `build_column_aliases`, `select_columns`, `filter_conditions` e `filter_selectors`.                                   |
| `toolkit.telemetry`                  | O `logger` do pacote e o `OpenTelemetry`.                                                                                                                                       |
| `toolkit.constants`, `toolkit.types` | Constantes e apelidos de tipo compartilhados.                                                                                                                                   |
| `toolkit.chunks`                     | Os chunks de um `dump` e o `hash` da `config`. Funções puras.                                                                                                                   |
| `toolkit.delta`                      | A tabela `Delta` de um `dump`: o `ledger`, a troca de um chunk, a soma de linhas, a compactação, a manutenção e a leitura do estado (`inspect_table` e `find_layout_problems`). |
| `toolkit.gcs`                        | `GCS` com `obstore`. Único lugar que importa `obstore`.                                                                                                                         |
| `toolkit.bigquery`                   | Cliente do BigQuery: instruções e tabelas externas.                                                                                                                             |
| `toolkit.oracle`                     | `Oracle`, o cliente `SQL` do `oracledb`: `connect`, `execute` e `query`.                                                                                                        |
| `toolkit.dbt`                        | O comando `dbt` de uma pipeline (`DbtRun`) e o clone do repositório. O `dbt` roda em `src/utils/tasks.py`.                                                                      |

- `__init__.py` fica vazio. O pacote é plano, e um módulo leva o nome do domínio, nunca o de uma biblioteca (`logging.py`, `polars.py`).
- Sem prefixo `_` em nomes. Uma função não recebe um parâmetro que não usa.
- Use `dataclass` para dados e `Pydantic` só em fronteiras. Alternativas fechadas são uniões etiquetadas tratadas com `match`. Um fluxo de vários passos é uma `dataclass` de contexto com um método por passo.
- Incorpore uma função auxiliar de uma linha que tem um só chamador. Ponha a função no módulo do domínio, e use `utils.py` só quando nenhum serve.
- Parâmetros explícitos e funções com nome. Evite `**kwargs`, lambdas opacas, `Any` e `object`. Código bloqueante roda por `asyncio.to_thread`.
- Cada parte tem a sua classe de configuração, que estende `ToolkitSettings`. A parte cria o grupo quando precisa dele, como `GcsSettings()`, nunca no `import`. A base só tem o que todas as partes usam: `LOG_LEVEL` e `OTEL_*`. Uma variável nova vai no grupo da parte que a usa.
- Constantes ficam em `constants.py` e apelidos de tipo em `types.py`, um de cada por pacote. Um módulo importa do próprio pacote e dos acima, nunca dos abaixo. `toolkit/types.py` não importa nada do toolkit. Classes ficam com o comportamento delas.
- Cada caminho de importação é um contrato, sem versionamento. Renomear, mover ou mudar o retorno de um símbolo público quebra os consumidores: atualize todos no mesmo PR, sem reexportação. Adicionar função, ou parâmetro com default, é seguro.

### 2.1 Documentação e comentários

- Escreva em inglês técnico simples (frases curtas, voz ativa) e em Markdown: código com uma crase, sem `reST` (`:param`, `:func:`, `::`).
- Todo módulo de produção tem `docstring` de módulo. Um teste e um `__init__.py` não têm. Toda função tem `docstring` (aninhada, mágica, de teste ou de dublê). Toda classe de produção também.
- A `docstring` é uma frase curta no imperativo, em uma linha, com ponto. Escreva mais frases só se o contrato não aparecer no nome e nos tipos, após uma linha em branco e em linhas de até 88 colunas. O erro que a função levanta entra na `docstring`. Uma classe leva um sintagma nominal, como "Um chunk de dados", e uma função com `@overload` não leva `docstring`.
- Uma função difícil de entender leva um exemplo em bloco `python` cercado, com o resultado num comentário `#` conferido ao rodar. Sem o rótulo `Example:`. Uma função do `DuckDB` leva o exemplo em bloco `sql`. Um auxiliar de teste difícil de ler também leva exemplo.
- Sem comentário no corpo de função e sem comentário de seção. Dê um nome à ideia.
- O `README.md` é o dono do uso e da configuração, e este guia é o dono das regras. Aponte com um link, não copie. Uma configuração nova entra na tabela do `README.md` na mesma alteração.

## 3. SQL e `DuckDB`

- Todo SQL é um modelo `Jinja` (`StrictUndefined`) e roda pelo `Executor`: `toolkit/executor/templates/<pasta>/<nome>.sql` no toolkit, `queries/*.sql` em cada pipeline. Não há SQL em Python, e ninguém abre conexão `DuckDB` fora do `Executor`.
- Valores entram como `Identifier()` e `Literal()`, e valores de execução como parâmetros (`?`). Nunca ponha um valor de fora, nem uma senha, no texto do SQL. Em SQL, carregue e apague em conjuntos.
- Uma conexão `DuckDB` atende uma consulta por vez: abra uma por tarefa concorrente.
- `Executor.query` e `Executor.read` conferem cada lote contra `expect`: uma classe anotada, um `polars.Schema`, um mapa de tipos ou `None`. O contrato (nulos, geometria, texto lido como número) está na `docstring` de `Executor.query` e no [README](README.md). Faça o `CAST` de texto para número no modelo, nunca na leitura.
- O `DuckDB` cuida de SQL, de leitura e escrita de Parquet e CSV, de `COPY` para o `GCS` e de geometria. O `Polars` cuida só do que o `DuckDB` não faz. Uma função pública recebe e devolve `Path`, `pl.DataFrame` ou `pl.LazyFrame`, nunca conexão, relação ou cursor.

- A cópia de uma fonte para o `bucket` extrai um chunk por vez com `COPY` por `Executor.execute` (`duckdb/extract_chunk`) para um `Parquet` local, e o `deltalake` grava a tabela `Delta`. Tudo que vira uma tabela `BigLake` é `Delta`. Do BigQuery para o `bucket` são lotes simples do `query` da fonte (`duckdb/select_source`). CSV só entra quando não há alternativa.

### 3.1 Modelos

- Todo modelo começa com um cabeçalho JSON em `{# ... #}` (`kind`, `description`, `inputs`). Toda macro tem `kind: "macro"`, `name`, `description`, `inputs` e `returns`, com uma linha em branco antes do cabeçalho, nenhuma entre ele e a macro, e uma depois.
- Um nome por papel (`secret`, `alias`, `path`, `columns`), sem `name` solto nem abreviação (`cols`). O mapeamento do Python e o modelo usam os mesmos nomes. O Python decide identificador, literal e parâmetro, e o `Jinja` só faz laços e condicionais.
- Cada valor de `.sqlfluff` é um exemplo válido de um modelo que existe.
- Uma operação que se repete vira função do `DuckDB` ou modelo em `duckdb/`, e não código em cada pipeline.
- Toda função do `DuckDB` fica em `toolkit/functions/`, no módulo do tipo: `native.py` (o `DuckDB` a chama uma vez por linha, com `@native`), `arrow.py` ou `documents.py` (uma vez por lote, com `@arrow`). Escreva a função como uma expressão do `Polars`.
- A função se registra onde é definida, com os decoradores de `decorators.py`. O `register_functions` a registra quando o `DuckDBEngine` é criado, então toda conexão a tem. A lista está no [README](README.md#módulos).
- Os modelos prontos são `extract_chunk`, `select_source`, `fingerprint` e `select_columns`.
- O nome de uma coluna usa `Executor.normalize_columns(table, columns, separator, lowercase)`, porque o `DuckDB` não aceita o resultado de uma função como nome de coluna. Ela renomeia as colunas dadas (ou todas, se `columns` for `None`) de uma tabela da própria conexão, nunca de uma fonte anexada.
- Na cópia de um banco, `select_columns` (em `toolkit/utils.py`) monta a lista de colunas da macro `select_columns`: `select`, nomes, `clean_text` e funções por coluna. Só entram funções `Arrow`, que leem texto, então `select_columns` converte datas e `timestamps` antes.

## 4. `Engine` e fontes

- Uma `Source` é só configuração: a conexão, as opções e a `origin` (o que ler). Um `Engine` nasce dela, com `Engine.from_source(source)`, abre a conexão no `async with` e faz tudo com o sistema. O `Executor` recebe o motor e orquestra: abre o motor, renderiza os modelos e os roda, e lê os chunks de um `dump` (`chunks`). Uma `Source` nunca abre uma conexão, e nunca monta nem roda uma consulta.
- A pasta `templates` de cada pipeline é procurada antes da do toolkit, então ela não usa nomes de pasta do toolkit, como `duckdb/`.
- O banco anexado se chama sempre `src` (`ALIAS`), e o modelo escreve `FROM src.esquema.tabela`. Um `DuckDBEngine` anexa uma fonte só. O SQL é sempre `DuckDB`. Uma consulta no dialeto de um banco vai como texto para a função da extensão dele (`postgres_query`, `mysql_query` ou `mssql_scan`), pela macro `native_query`. O Oracle é a exceção: o `OracleEngine` guarda um `Oracle` próprio, o `Executor` roda nele os modelos de `oracle/` (`prepare` ajusta o fuso e lê o `SCN`), e `materialize` grava cada chunk em `Parquet` local para um `DuckDBEngine` do próprio motor terminar. O CouchDB e o MongoDB são as outras exceções: não têm SQL, então o `CouchDBEngine` guarda um `CouchDBClient` (`executor/couchdb.py`, o protocolo `HTTP`) e o `MongoDBEngine` um cliente do `PyMongo`, e cada um grava as páginas em `Parquet`. O motor faz tudo com o sistema (`execute`, `query`, `read`, `describe` e `materialize`, com `match` no tipo da origem), e `materialize` é o único ponto em que a leitura aplica os filtros e o chunk.
- O `Engine` é uma classe abstrata em `engines/base.py`, e cada motor é uma `dataclass` cujo primeiro campo é a fonte. Ele define `source_type` (as fontes que serve) e, se lê chunks ao mesmo tempo, `concurrency`. O `from_source` acha o motor pelo `isinstance` da fonte, então um motor novo não muda nenhum outro arquivo. Um motor que corta os chunks à sua maneira sobrescreve `check_chunks`. Todo motor implementa o `Engine` direto, sem classe intermediária. Um sistema que o `DuckDB` não lê escreve o `extract` (as linhas do chunk em um `Parquet`), abre um `DuckDBEngine` próprio e termina com `describe_extracted` e `materialize_extracted` (`engines/duckdb.py`). `execute` e `query` recusam o SQL por padrão, e o motor que roda SQL os sobrescreve.
- Uma fonte é uma classe por arquivo em `sources/backends/`, subclasse de `Registered` (ou de `DuckDBSource`, para o que o `DuckDB` lê no lugar), com o nome terminando em `Source`, um `name` e o decorador `@dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)` do `Pydantic`. Os campos são as opções, e `extra="forbid"` recusa uma chave que ela não declara. Só uma classe que define `name` entra no registro, e a intermediária não define. Um sistema novo é um arquivo novo.
- Uma fonte que o `DuckDB` lê no lugar define `extensions` e, se precisa, `setup(engine)`. Um motor só carrega as extensões e roda o `setup`.
- Os modelos `Pydantic` das respostas que uma fonte lê de uma API HTTP ficam em `sources/models.py`: a fronteira valida o corpo, e o resto do código usa os campos tipados, sem `cast`.
- A conexão (`DBConn`) é uma `dataclass` congelada, com a senha fora do `repr`. Nunca registre em log senha, token, URL de conexão, valores ou texto de consulta.
- A construção de uma `Source` e a de um `Engine` não têm efeito colateral: a conexão abre no `async with`.

## 5. Google Cloud

- Autenticação por workload identity (ADC): `google.auth.default(scopes=...)`. Sem arquivo de service account, sem `GOOGLE_APPLICATION_CREDENTIALS` e sem credencial em imagem.
- HTTP usa `httpx2`, e `requests` é proibido.
- O `GCS` só passa por `toolkit.gcs` (`obstore`, chaves `HMAC` de `Settings`). Ele não apaga em lote: apague objeto por objeto, com concorrência limitada. O `DuckDB` não acessa o `GCS`: no `dump` de um banco ele grava `Parquet` local e o `deltalake` escreve a tabela `Delta` no `bucket`, pelo ADC.
- O BigQuery só passa por `toolkit.bigquery`. A tabela final é uma tabela externa sobre o prefixo, criada depois que os dados existem. Uma tabela `Delta` é uma tabela `BigLake` e exige uma conexão.
- O `dump` de um banco não repete dado: cada chunk tem uma impressão digital no `ledger` da tabela `Delta` (`toolkit.delta`), e só um chunk que mudou é trocado, em um `commit` atômico. O `md5` dá a impressão digital (`hash()` do `DuckDB` pode mudar entre versões). `deltalake` só em `toolkit/delta.py`.

## 6. `Prefect`, logs e telemetria

- O toolkit tem funções puras: sem `@flow`, `@task`, `if __name__ == "__main__":` nem `print()`. O `@task` é uma função fina, no `tasks.py` da pipeline ou em `src/utils/tasks.py`, que decide `retries` e cache.
- Todo log passa por `toolkit.telemetry.logger` (`prefect.toolkit`), no formato `<Componente> <ação> <estado>: contexto`, com o nível do guia do repositório (Seção 5.5). O nível vem só de `Settings.LOG_LEVEL`. A saída de ferramentas externas fica como vem.
- Uma operação (consulta, instrução, envio, exclusão) é um `span`, com o nome em `constants.py`. Não há histograma de duração, porque o `SigNoz` a deriva dos `spans`, e não se decora `flow` nem `task`.
- Um instrumento de `telemetry.py` é só para totais que um `span` não dá (linhas, bytes), com unidade e atributos de cardinalidade baixa. Valor de dado, id de linha e mensagem de erro não são atributos.
- O pacote usa só a API do `OpenTelemetry`. Os `flows` compartilhados de `utils.flows` chamam `configure_telemetry()` ao serem importados, depois de `import prefect`; um `flow` próprio faz o mesmo. Ela cria `ToolkitSettings` (`OTEL_*`) e não recebe as configurações como parâmetro.

## 7. Testes

Teste só o nosso comportamento, nunca o do `DuckDB`, `Polars`, `Prefect`, `OpenTelemetry`, `obstore`, `psycopg`, `Jinja` ou Google. As regras gerais estão na [Seção 14 do guia do repositório](../../STYLEGUIDE.md#14-testes).

- `tests/` espelha o pacote, com funções `test_*` simples: sem `pytest-bdd` e sem pastas `unit/` e `integration/`. A marca `s3` ou `extensions` identifica o teste que precisa de serviço ou rede. O que os testes compartilham vai no `conftest.py` (na raiz do repositório, em `tests/fixtures.py`), como `fixture` ou como fábrica de funções. Um módulo de teste só tem testes e seus dados: constantes, classes de esquema e o auxiliar que um `parametrize` ou um teste `@given` usa na coleta, porque o `pytest` e o `hypothesis` não passam `fixture` a eles.
- Um teste é pequeno, e o nome diz o comportamento. Prefira tabelas com `pytest.param(..., id="cenário")`, e `hypothesis` para invariantes, não para um exemplo só.
- Confira valores exatos, sem `in` sobre texto. Use `DuckDB` em memória e `tmp_path`, sem dublê de `DuckDB`. Faça dublê só do externo que não roda aqui (cliente do `Prefect`, chamada ao `dbt`, `oracledb` por SQLite, token do Google), com a forma mínima.
- Todo bug ganha um teste de regressão, com o cenário no `id` e `Regression` na `docstring`. Estrague o código de propósito e veja o teste falhar.
- Os testes de paridade (`tests/parity/` na raiz, marca `parity`, desligada por padrão) rodam os seis motores de verdade e conferem que todos dão as mesmas linhas e os mesmos tipos. Um motor novo entra neles, com o `engines.nu` que o sobe.
- Não se testa: logs, `spans` e métricas (confira à mão em `/tmp`), o BigQuery, o armazenamento de campos, os padrões e o "não levantou", o conteúdo inteiro de uma lista que cresce (o registro de fontes), e o mesmo comportamento em dois níveis.

## 8. Verificações

Rode em `packages/toolkit/`:

```sh
uv run ruff check . && uv run ruff format --check .
uv run basedpyright
uv run sqlfluff lint toolkit/executor/templates
uv run vulture toolkit tests
uv run pytest
```

| Regra                                                                                                                   | Ferramenta                             |
| ----------------------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| Sem `pandas`, `geopandas`, `requests`, `basedosdados`, `google.cloud.storage`, `boto3`, `psycopg2`, `pymysql`, `pyodbc` | `ruff` (`TID251`)                      |
| `obstore` só em `toolkit/gcs.py`                                                                                        | `ruff` (`TID251`, exceção por arquivo) |
| Sem `print`                                                                                                             | `ruff` (`T20`)                         |
| `docstring` em toda função, com resumo de uma linha no imperativo                                                       | `ruff` (`D`)                           |
| Linha de `docstring` e de comentário com até 88 colunas                                                                 | `ruff` (`W505`)                        |
| Tipos completos: modo `all` e nível `strict`                                                                            | `basedpyright`                         |
| Estilo dos modelos SQL                                                                                                  | `sqlfluff`                             |
| Código sem uso                                                                                                          | `vulture`                              |

O verificador de tipos cobre `toolkit/` e `tests/` com zero erros e zero avisos. Não há comentário de ignore no código (`# pyright: ignore`, `# type: ignore`). Toda supressão vive no `pyproject.toml`, no escopo mais estreito (um arquivo ou uma pasta), com o motivo em um comentário, e só uma lacuna de tipos de terceiros a justifica. Ela é uma `executionEnvironments` com `root`.
