---
lang: pt-br
---

# Guia de Estilo do toolkit

Regras para escrever, estruturar e manter o pacote `toolkit`. Este guia define como o pacote deve ser escrito: DuckDB como motor de SQL e de E/S, Polars para transformações em memória.

O `toolkit` segue integralmente o [`STYLEGUIDE.md`](../../STYLEGUIDE.md) da raiz: estilo Python, dados tabulares, validação, SQL, testes e higiene do repositório. Este arquivo tem apenas as regras que só existem no `toolkit`.

## Índice

1. [Filosofia](#1-filosofia)
2. [Critérios de promoção](#2-critérios-de-promoção)
3. [Layout alvo](#3-layout-alvo)
4. [API pública e contratos de import](#4-api-pública-e-contratos-de-import)
5. [Camada de dados: DuckDB e Polars](#5-camada-de-dados-duckdb-e-polars)
6. [Fronteira com o Prefect](#6-fronteira-com-o-prefect)
7. [Testes do toolkit](#7-testes-do-toolkit)

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

## 3. Layout alvo

### 3.1 Módulos

O pacote é plano. Cada módulo cobre um domínio técnico e leva o nome do domínio.

| Módulo                | Responsabilidade                                                     | Papel de DuckDB e Polars                                   |
| --------------------- | -------------------------------------------------------------------- | ---------------------------------------------------------- |
| `toolkit.env`         | Credenciais e variáveis de ambiente (Infisical).                     | Nenhum.                                                    |
| `toolkit.prefect`     | Helpers de execução do Prefect, como renomear o flow run.            | Nenhum.                                                    |
| `toolkit.bd`          | Criação de tabela e upload para BigQuery e GCS, a partir de Parquet. | Lê Parquet.                                                |
| `toolkit.gcs`         | Listar, enviar e apagar objetos no GCS.                              | Nenhum.                                                    |
| `toolkit.dbt`         | Execução de comandos dbt.                                            | Nenhum.                                                    |
| `toolkit.db`          | Conectores de banco (Oracle, Postgres, MySQL, SQL Server) e leitura. | DuckDB lê as fontes. O driver cobre o que o DuckDB não lê. |
| `toolkit.frames`      | Transformações de DataFrame.                                         | Polars.                                                    |
| `toolkit.dump_db`     | Template de extração de banco para o BigQuery.                       | DuckDB e Polars, via `db` e `frames`.                      |
| `toolkit.dump_url`    | Template de extração de URL, Google Sheets e Google Drive.           | Polars.                                                    |
| `toolkit.dump_arcgis` | Template de extração do ArcGIS.                                      | Polars.                                                    |

### 3.2 Regras de nomes e módulos

- Um módulo cobre exatamente um domínio técnico. Não crie `utils.py`, `core.py` ou `common.py`.
- Não dê a um módulo o nome de uma biblioteca padrão ou de terceiros, como `logging.py` ou `polars.py`. O nome confunde os imports.
- Cada constante vive no módulo que a usa. Não existe um módulo só de constantes.
- Os módulos `dump_*` são subpacotes com `tasks.py` e `utils.py`. O SQL deles fica em `queries/` e é carregado com `load_query` (guia raiz, Seção 7).

```
toolkit/dump_db/
├── tasks.py      ← só @task, delega para utils.py
├── utils.py      ← lógica pura
└── queries/
```

## 4. API pública e contratos de import

### 4.1 Caminhos de import são contratos

O `toolkit` é usado por todas as pipelines sem versionamento semântico. Cada caminho de import é um contrato. Renomear ou mover um símbolo público quebra os consumidores sem aviso.

```python
# Consumidor depende deste import exato
from toolkit.bd import create_table_and_upload_to_gcs_task
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

## 5. Camada de dados: DuckDB e Polars

### 5.1 Papéis

| Ferramenta | Papel                                                                                         |
| ---------- | --------------------------------------------------------------------------------------------- |
| DuckDB     | Motor de SQL, de leitura de fontes e de E/S de Parquet.                                       |
| Polars     | Transformações de DataFrame em memória.                                                       |
| Parquet    | Único formato entre etapas. CSV só quando a API de destino exigir, com o motivo na docstring. |

### 5.2 Assinaturas públicas

Uma função pública recebe e devolve `Path` de um Parquet, `pl.DataFrame` ou `pl.LazyFrame`. Ela nunca expõe `pd.DataFrame`, conexão ou relação do DuckDB, nem cursor de driver.

```python
# ✅ correto
def clean_columns(source: Path, dest: Path) -> Path:
    """Normaliza os nomes das colunas de um Parquet.

    :param source: Parquet de entrada.
    :param dest: Parquet de saída.
    :returns: O caminho ``dest``.
    """
    ...

# ❌ errado — tipo de biblioteca interna no contrato público
def clean_columns(rel: duckdb.DuckDBPyRelation) -> pd.DataFrame:
    ...
```

### 5.3 Conexões DuckDB

Abra uma conexão curta por chamada, com `with`. Não crie conexão no nível do módulo.

```python
# ✅ correto
from pathlib import Path

import duckdb
from prefect_rj_iplanrio.sql import load_query


def count_rows(source: Path) -> int:
    """Conta as linhas de um Parquet.

    :param source: Parquet de entrada.
    :returns: Número de linhas.
    """
    query = load_query(__file__, "count_rows", source=source)
    with duckdb.connect() as con:
        return con.execute(query).fetchone()[0]

# ❌ errado — conexão global compartilhada entre chamadas e threads
CON = duckdb.connect()
```

```sql
-- queries/count_rows.sql
SELECT count(*) FROM read_parquet('$source')
```

### 5.4 Fontes de dados

- **Fonte com scanner DuckDB (Postgres, MySQL):** o DuckDB lê a fonte direto e escreve o Parquet.
- **Fonte sem scanner (Oracle, SQL Server):** o driver busca as linhas em lotes (`fetchmany`), cada lote vira um `pl.DataFrame` e é escrito em um Parquet.
- Todo dump escreve Parquet em lotes de tamanho limitado. A memória é limitada por `batch_size`, não pelo tamanho da tabela.
- Dados particionados usam `COPY ... TO ... (FORMAT PARQUET, PARTITION_BY (...))` do DuckDB.
- A conexão de banco é um `dataclass(frozen=True)` (guia raiz, Seção 5.7). A senha nunca entra em log nem em string SQL.

```python
# ✅ correto — a função recebe a configuração e devolve os arquivos escritos
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class DbSource:
    """Parâmetros de conexão de um banco relacional."""

    database_type: str
    hostname: str
    port: int
    user: str
    password: str = field(repr=False)
    database: str


def dump_query_to_parquet(source: DbSource, query: str, dest_dir: Path, batch_size: int) -> list[Path]:
    """Executa a query e escreve o resultado em arquivos Parquet por lote.

    :param source: Conexão com o banco.
    :param query: SQL já renderizado.
    :param dest_dir: Diretório de saída.
    :param batch_size: Número máximo de linhas por arquivo.
    :returns: Os arquivos Parquet escritos, em ordem.
    """
    ...
```

### 5.5 Upload

`toolkit.bd` recebe apenas Parquet. Nenhuma função pública de upload aceita DataFrame de pandas.

## 6. Fronteira com o Prefect

Uma função pura e o `@task` que a envolve ficam no mesmo módulo. O `@task` é um wrapper fino e a lógica fica na função pura.

```python
# ✅ correto
def create_table_and_upload_to_gcs(data_path: Path, dataset_id: str, table_id: str, dump_mode: str) -> Path:
    """Cria a tabela no BigQuery e envia os dados para o GCS."""
    ...


@task
def create_table_and_upload_to_gcs_task(data_path: Path, dataset_id: str, table_id: str, dump_mode: str) -> Path:
    """Wrapper Prefect de ``create_table_and_upload_to_gcs``."""
    return create_table_and_upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
    )
```

| Padrão                                            | Motivo                                                            |
| ------------------------------------------------- | ----------------------------------------------------------------- |
| `@flow` em qualquer arquivo do pacote             | Um flow aqui tem parâmetros fixos e vira uma pipeline disfarçada. |
| Lógica de negócio dentro do `@task`               | A lógica presa ao wrapper não pode ser testada sem o Prefect.     |
| `@task` fora de módulos com essa responsabilidade | Dificulta achar onde estão as tasks.                              |
| `if __name__ == "__main__":`                      | Scripts avulsos não pertencem a uma biblioteca.                   |

## 7. Testes do toolkit

A pasta `packages/toolkit/tests/` espelha o pacote. Cada módulo tem um arquivo de teste com o mesmo caminho, prefixado por `test_`. Um arquivo de teste nunca testa dois módulos.

```
toolkit/frames.py        →  tests/test_frames.py
toolkit/db.py            →  tests/test_db.py
toolkit/dump_db/utils.py →  tests/dump_db/test_utils.py
```

As regras gerais de teste estão na [Seção 14 do guia raiz](../../STYLEGUIDE.md#14-testes). Regras só do toolkit:

- Testes de DuckDB usam `duckdb.connect(":memory:")` e arquivos em `tmp_path`.
- Funções que chamam GCS, BigQuery ou bancos não têm teste de integração. Isole a lógica pura e teste só ela.
- Propriedades verificadas com `hypothesis`:

| Propriedade                                       | Função                     | Estratégia                                   |
| ------------------------------------------------- | -------------------------- | -------------------------------------------- |
| Normalizar nomes de colunas duas vezes dá o mesmo | `frames.normalize_columns` | `st.lists(st.text(min_size=1), unique=True)` |
| Um lote de N linhas vira um DataFrame de N linhas | `frames.batch_to_frame`    | `st.integers(min_value=1, max_value=1000)`   |
| Escrever e ler um Parquet preserva o schema       | `frames.write_parquet`     | `st.fixed_dictionaries(...)`                 |

```python
from pathlib import Path

import polars as pl
import pytest

from toolkit.db import count_rows


@pytest.mark.parametrize(("rows", "expected"), [(0, 0), (3, 3)], ids=["empty", "three_rows"])
def test_count_rows(tmp_path: Path, rows: int, expected: int) -> None:
    path = tmp_path / "data.parquet"
    pl.DataFrame({"id": range(rows)}).write_parquet(path)
    assert count_rows(path) == expected
```
