---
lang: pt-br
---

# Guia de Estilo do toolkit

Regras só do pacote `toolkit`. Ele segue o [guia do repositório](../../STYLEGUIDE.md) (Python, dados, testes, repositório) e parte do `data-proxy`: siga um padrão existente antes de criar outro. O comportamento de cada função está na `docstring` dela, não neste guia.

## 1. Escopo

O `toolkit` tem primitivos reutilizáveis e nenhuma lógica de domínio. Regras de negócio, consultas, constantes de `dataset` ou `table_id`, `@flow` e `@task` ficam em cada pipeline.

Promova uma função para cá só no segundo uso confirmado, e só se ela for genérica, estável e sem lógica de pipeline. Promoção especulativa é proibida.

## 2. Layout e código

| Módulo                               | Responsabilidade                                                                                                                                    |
| ------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `toolkit.executor`                   | `Executor`, `Engine`, fontes e modelos SQL.                                                                                                         |
| `toolkit.settings`                   | `Settings` e credenciais de banco.                                                                                                                  |
| `toolkit.functions`                  | As funções do `DuckDB`: `native.py` (uma vez por linha), `arrow.py` e `documents.py` (uma vez por lote) e `registry.py`, com o registro automático. |
| `toolkit.utils`                      | As colunas e os filtros de um `dump`: `normalize_column`, `column_aliases`, `select_columns` e `filter_predicates`.                                 |
| `toolkit.telemetry`                  | O `logger` do pacote e o `OpenTelemetry`.                                                                                                           |
| `toolkit.constants`, `toolkit.types` | Constantes e apelidos de tipo compartilhados.                                                                                                       |
| `toolkit.batches`                    | As janelas de um `dump` e o `hash` da `config`. Funções puras.                                                                                      |
| `toolkit.delta`                      | A tabela `Delta` de um `dump`: o `ledger`, a troca de uma janela e a manutenção.                                                                    |
| `toolkit.gcs`                        | `GCS` com `obstore`. Único lugar que importa `obstore`.                                                                                             |
| `toolkit.bigquery`                   | Cliente do BigQuery e tabelas externas.                                                                                                             |
| `toolkit.dbt`                        | Execução de `dbt`.                                                                                                                                  |

- `__init__.py` fica vazio. O pacote é plano, e um módulo leva o nome do domínio, nunca o de uma biblioteca (`logging.py`, `polars.py`).
- Sem prefixo `_` em nomes. Uma função não recebe um parâmetro que não usa.
- Use `dataclass` para dados e `Pydantic` só em fronteiras. Alternativas fechadas são uniões etiquetadas tratadas com `match`. Um fluxo de vários passos é uma `dataclass` de contexto com um método por passo.
- Incorpore uma função auxiliar de uma linha que tem um só chamador. Ponha a função no módulo do domínio, e use `utils.py` só quando nenhum serve.
- Parâmetros explícitos e funções com nome. Evite `**kwargs`, lambdas opacas, `Any` e `object`. Código bloqueante roda por `asyncio.to_thread`.
- Constantes ficam em `constants.py` e apelidos de tipo em `types.py`, um de cada por pacote. Um módulo importa do próprio pacote e dos acima, nunca dos abaixo. `toolkit/types.py` não importa nada do toolkit. Classes ficam com o comportamento delas.
- Cada caminho de importação é um contrato, sem versionamento. Renomear, mover ou mudar o retorno de um símbolo público quebra os consumidores: atualize todos no mesmo PR, sem reexportação. Adicionar função, ou parâmetro com default, é seguro.

### 2.1 Documentação e comentários

- Escreva em inglês técnico simples (frases curtas, voz ativa) e em Markdown: código com uma crase, sem `reST` (`:param`, `:func:`, `::`).
- Todo módulo de produção tem `docstring` de módulo. Um teste e um `__init__.py` não têm. Toda função tem `docstring` (aninhada, mágica, de teste ou de dublê). Toda classe de produção também.
- A `docstring` é uma frase curta no imperativo, em uma linha, com ponto. Escreva mais frases só se o contrato não aparecer no nome e nos tipos, após uma linha em branco e em linhas de até 88 colunas. O erro que a função levanta entra na `docstring`.
- Uma função difícil de entender leva um exemplo em bloco `python` cercado, com o resultado num comentário `#` conferido ao rodar. Sem o rótulo `Example:`.
- Sem comentário no corpo de função e sem comentário de seção. Dê um nome à ideia.
- O `README.md` é o dono do uso e da configuração, e este guia é o dono das regras. Aponte com um link, não copie. Uma configuração nova entra na tabela do `README.md` na mesma alteração.

## 3. SQL e `DuckDB`

- Todo SQL é um modelo `Jinja` (`StrictUndefined`) e roda pelo `Executor`: `toolkit/executor/templates/<pasta>/<nome>.sql` no toolkit, `queries/*.sql` em cada pipeline. Não há SQL em Python, e ninguém abre conexão `DuckDB` fora do `Executor`.
- Valores entram como `Identifier()` e `Literal()`, e valores de execução como parâmetros (`?`). Nunca ponha um valor de fora, nem uma senha, no texto do SQL. Em SQL, carregue e apague em conjuntos.
- Uma conexão `DuckDB` atende uma consulta por vez: abra uma por tarefa concorrente.
- `Executor.query(..., expect=Schema)` devolve um `Dataset` tipado. O esquema é uma classe anotada, `X | None` marca nulos e `Geometry` marca geometria. Um texto nunca é lido como número: faça o `CAST` no modelo.
- O `DuckDB` cuida de SQL, de leitura e escrita de Parquet e CSV, de `COPY` para o `GCS` e de geometria. O `Polars` cuida só do que o `DuckDB` não faz. Uma função pública recebe e devolve `Path`, `pl.DataFrame` ou `pl.LazyFrame`, nunca conexão, relação ou cursor.

- A cópia de uma fonte para o `bucket` extrai uma janela por vez com `COPY` por `Executor.execute` (`duckdb/extract_window`) para um `Parquet` local, e o `deltalake` grava a tabela `Delta`. Tudo que vira uma tabela `BigLake` é `Delta`. Do BigQuery para o `bucket` são lotes simples do `query` da fonte (`duckdb/select_source`). CSV só entra quando não há alternativa.

### 3.1 Modelos

- Todo modelo começa com um cabeçalho JSON em `{# ... #}` (`kind`, `description`, `inputs`). Toda macro tem `kind: "macro"`, `name`, `description`, `inputs` e `returns`, com uma linha em branco antes do cabeçalho, nenhuma entre ele e a macro, e uma depois.
- Um nome por papel (`secret`, `alias`, `path`, `columns`), sem `name` solto nem abreviação (`cols`). O mapeamento do Python e o modelo usam os mesmos nomes. O Python decide identificador, literal e parâmetro, e o `Jinja` só faz laços e condicionais.
- Cada valor de `.sqlfluff` é um exemplo válido de um modelo que existe.
- Operação que se repete vira função do `DuckDB` ou modelo em `duckdb/`, e não código em cada pipeline. Toda função do `DuckDB` fica em `toolkit/functions/`: o código no módulo do tipo, `native.py` (o `DuckDB` a chama uma vez por linha, com `@native`), `arrow.py` ou `documents.py` (uma vez por lote, com `@arrow`). A função se registra onde é definida, com os decoradores de `decorators.py`. O `get_functions`, de `registry.py`, importa os módulos do pacote e devolve `(arrow, native)`, e o `register_functions` registra cada item com `create_function` quando o `DuckDBEngine` é criado, então toda conexão as tem: `normalize`, `clean_text`, `parse_date`, `parse_timestamp`, `ano_particao`, `mes_particao`, `data_particao`, `now_sao_paulo`, `to_sao_paulo`, `is_cpf`, `normalize_cpf`, `is_cnpj`, `normalize_cnpj` e `is_phone`. Escreva a função como uma expressão do `Polars` e a decore com `@arrow`, porque o `DuckDB` a chama uma vez por lote, e não uma vez por linha. Os modelos prontos são `extract_window`, `select_source`, `fingerprint` e `select_columns`. O nome de uma coluna usa `Executor.normalize_columns(table, columns, separator, lowercase)`, porque o `DuckDB` não aceita o resultado de uma função como nome de coluna. Ela renomeia as colunas dadas, ou todas se `columns` for `None`, de uma tabela da própria conexão, nunca de uma fonte anexada. Na cópia de um banco, `select_columns` (em `toolkit/utils.py`) monta a lista de colunas para a macro `select_columns`: `select`, nomes, `clean_text` e funções por coluna. Só entram funções `Arrow`, e elas leem texto, então `select_columns` converte datas e `timestamps` antes.

## 4. `Engine` e fontes

- O cliente roda SQL (protocolo `Engine`: `execute` e `query`). A `Source` abre e prepara o cliente (`open` e `setup`). O `Executor` renderiza os modelos e os roda. Uma `Source` nunca monta nem roda uma consulta.
- A pasta `templates` de cada pipeline é procurada antes da do toolkit, então ela não usa nomes de pasta do toolkit, como `duckdb/`.
- O banco anexado se chama sempre `src` (`ALIAS`), e o modelo escreve `FROM src.esquema.tabela`. Um `Engine` anexa uma fonte só. O SQL é sempre `DuckDB`. Uma consulta no dialeto de um banco vai como texto para a função da extensão dele (`postgres_query`, `mysql_query`, `mssql_scan` ou `oracle_query`), pela macro `native_query`.
- Uma fonte é uma classe por arquivo em `sources/backends/`, subclasse de `SourceBase`, com o nome terminando em `Source`. O `autoregistry` a registra, e uma classe intermediária usa `skip=True`. Um sistema novo é um arquivo novo. Uma subclasse de `DuckDBSource` só define atributos `ClassVar`.
- A conexão (`DBConn`) é uma `dataclass` congelada, com a senha fora do `repr`. Nunca registre em log senha, token, URL de conexão, valores ou texto de consulta.
- A construção de uma `Source` não tem efeito colateral: o cliente abre no `open`.

## 5. Google Cloud

- Autenticação por workload identity (ADC): `google.auth.default(scopes=...)`. Sem arquivo de service account, sem `GOOGLE_APPLICATION_CREDENTIALS` e sem credencial em imagem.
- HTTP usa `httpx2`, e `requests` é proibido.
- O `GCS` só passa por `toolkit.gcs` (`obstore`, chaves `HMAC` de `Settings`). Ele não apaga em lote: apague objeto por objeto, com concorrência limitada. O `DuckDB` não acessa o `GCS`: no `dump` de um banco ele grava `Parquet` local e o `deltalake` escreve a tabela `Delta` no `bucket`, pelo ADC.
- O BigQuery só passa por `toolkit.bigquery`. A tabela final é uma tabela externa sobre o prefixo, criada depois que os dados existem. Uma tabela `Delta` é uma tabela `BigLake` e exige uma conexão.
- O `dump` de um banco não repete dado: cada janela tem uma impressão digital no `ledger` da tabela `Delta` (`toolkit.delta`), e só uma janela que mudou é trocada, em um `commit` atômico. O `md5` dá a impressão digital (`hash()` do `DuckDB` pode mudar entre versões). `deltalake` só em `toolkit/delta.py`.

## 6. `Prefect`, logs e telemetria

- O toolkit tem funções puras: sem `@flow`, `@task`, `if __name__ == "__main__":` nem `print()`. O `@task` é uma função fina no `tasks.py` de cada pipeline, que decide `retries` e cache.
- Todo log passa por `toolkit.telemetry.logger` (`prefect.toolkit`), no formato `<Componente> <ação> <estado>: contexto`, com o nível do guia do repositório (Seção 5.5). O nível vem só de `Settings.LOG_LEVEL`. A saída de ferramentas externas fica como vem.
- Uma operação (consulta, instrução, envio, exclusão) é um `span`, com o nome em `constants.py`. Não há histograma de duração, porque o `SigNoz` a deriva dos `spans`, e não se decora `flow` nem `task`.
- Um instrumento de `telemetry.py` é só para totais que um `span` não dá (linhas, bytes), com unidade e atributos de cardinalidade baixa. Valor de dado, id de linha e mensagem de erro não são atributos.
- O pacote usa só a API do `OpenTelemetry`. Os `flows` compartilhados de `prefect_rj_iplanrio.flows` chamam `configure_telemetry()` ao serem importados, depois de `import prefect`; um `flow` próprio faz o mesmo. Ela lê o `singleton` `settings` (`OTEL_*`) e não recebe `settings` como parâmetro.

## 7. Testes

Teste só o nosso comportamento, nunca o do `DuckDB`, `Polars`, `Prefect`, `OpenTelemetry`, `obstore`, `psycopg`, `Jinja`, `autoregistry` ou Google. As regras gerais estão na [Seção 14 do guia do repositório](../../STYLEGUIDE.md#14-testes).

- `tests/` espelha o pacote, com funções `test_*` simples: sem `pytest-bdd` e sem pastas `unit/` e `integration/`. A marca `s3` ou `extensions` identifica o teste que precisa de serviço ou rede. O que vários testes compartilham vai no `conftest.py`, e um módulo de teste só tem testes.
- Um teste é pequeno, e o nome diz o comportamento. Prefira tabelas com `pytest.param(..., id="cenário")`, e `hypothesis` para invariantes, não para um exemplo só.
- Confira valores exatos, sem `in` sobre texto. Use `DuckDB` em memória e `tmp_path`, sem dublê de `DuckDB`. Faça dublê só do externo que não roda aqui (cliente do `Prefect`, chamada ao `dbt`, `oracledb` por SQLite, token do Google), com a forma mínima.
- Todo bug ganha um teste de regressão, com o cenário no `id` e `Regression` na `docstring`. Estrague o código de propósito e veja o teste falhar.
- Não se testa: logs, `spans` e métricas (confira à mão em `/tmp`), o BigQuery, o armazenamento de campos, os padrões e o "não levantou", o conteúdo inteiro de uma lista que cresce (o registro de fontes), e o mesmo comportamento em dois níveis.

## 8. Verificações

Rode em `packages/toolkit/`:

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
| `docstring` em toda função, com resumo de uma linha no imperativo                                                       | `ruff` (`D`)                           |
| Tipos completos: modo `all` e nível `strict`                                                                            | `basedpyright`, `pyrefly`              |
| Estilo dos modelos SQL                                                                                                  | `sqlfluff`                             |
| Código sem uso                                                                                                          | `vulture`                              |

Os verificadores de tipos cobrem `toolkit/` e `tests/` com zero erros e zero avisos. Não há comentário de ignore no código (`# pyright: ignore`, `# pyrefly: ignore`, `# type: ignore`). Toda supressão vive no `pyproject.toml`, no escopo mais estreito (um arquivo ou uma pasta), com o motivo em um comentário, e só uma lacuna de tipos de terceiros a justifica. No `basedpyright` ela é uma `executionEnvironments` com `root`, e no `pyrefly` um `sub-config` com `matches` (só o primeiro que casa vale).
