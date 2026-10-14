---
lang: pt-br
---

# Guia de Estilo

Regras para escrever, estruturar e manter pipelines neste monorepo. Cada regra declara o que é, por que existe e apresenta um exemplo concreto.

As regras de Python, dados e testes valem também para `packages/toolkit/`. As regras só do toolkit estão em [`packages/toolkit/STYLEGUIDE.md`](packages/toolkit/STYLEGUIDE.md).

## Início rápido

Vai criar uma pipeline nova? Siga estes cinco passos. Cada passo aponta para a seção com a regra completa.

1. **Crie a pipeline com o template.** Rode `uvx cookiecutter templates --output-dir=pipelines`. O template cria o diretório com `flow.py`, `tasks.py`, `queries/`, `prefect.yaml`, `pyproject.toml` e `Dockerfile`. O nome segue o padrão `rj_<secretaria>__<pipeline>` ([Seção 3](#3-contrato-de-arquivos-da-pipeline)).
2. **Descreva a pipeline.** Escreva o `description` no `pyproject.toml` ([Seção 10](#10-pyprojecttoml)).
3. **Escreva o código nos arquivos certos.** O flow só orquestra, a task é um wrapper fino e o trabalho real fica em `utils.py` ([Seção 4](#4-responsabilidades-dos-módulos)). O SQL fica em `queries/*.sql` ([Seção 7](#7-sql)). Antes de escrever um helper, procure no [README do toolkit](packages/toolkit/README.md).
4. **Configure o deploy.** No `prefect.yaml`, defina as tags, o agendamento da produção e o work pool ([Seções 8, 9 e 13](#8-agendamento)).
5. **Teste e abra o PR.** Escreva testes para a lógica de `utils.py` ([Seção 14](#14-testes)) e passe pelo [checklist](#15-checklist-antes-do-pull-request).

Existem dois pontos de partida:

- **Pipeline de dump (banco para BigQuery):** use o template como ele vem. Você só edita `queries/extract_data.sql` e os parâmetros no `prefect.yaml`.
- **Pipeline própria:** adicione `utils.py` com a lógica e, se precisar, mais tasks em `tasks.py`.

### Exemplo mínimo completo

Uma pipeline que baixa uma lista de itens de uma API e salva em Parquet. Os comentários indicam a seção que explica cada arquivo.

```python
# pipelines/rj_secretaria__pipeline/flow.py  — Seção 4.1
"""Flow for rj_secretaria__pipeline."""

from prefect import flow

from pipelines.rj_secretaria__pipeline.tasks import (
    fetch_items_task,
    rename_flow_run_task,
)


@flow(log_prints=True)
def rj_secretaria__pipeline(
    dataset_id: str = "brutos_example",
    table_id: str = "items",
) -> None:
    rename_flow_run_task(new_name=table_id)
    fetch_items_task(dataset_id=dataset_id, table_id=table_id)
```

```python
# pipelines/rj_secretaria__pipeline/tasks.py  — Seção 4.2
"""Tasks for rj_secretaria__pipeline."""

from prefect import task
from toolkit.prefect import rename_current_flow_run

from pipelines.rj_secretaria__pipeline import utils


@task
async def rename_flow_run_task(new_name: str) -> None:
    """Renomeia a flow run atual."""
    await rename_current_flow_run(new_name)


@task
def fetch_items_task(dataset_id: str, table_id: str) -> str:
    """Baixa os itens e retorna o caminho do Parquet."""
    return utils.fetch_items(dataset_id=dataset_id, table_id=table_id)
```

```python
# pipelines/rj_secretaria__pipeline/utils.py  — Seções 4.3, 5.5 e 5.8
"""Funções puras da pipeline rj_secretaria__pipeline."""

import httpx2
import polars as pl
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


def fetch_items(dataset_id: str, table_id: str) -> str:
    """Baixa os itens da API e salva em Parquet.

    :param dataset_id: Dataset de destino no BigQuery.
    :param table_id: Tabela de destino no BigQuery.
    :returns: Caminho do arquivo Parquet.
    :raises httpx2.HTTPStatusError: Se a API responder com erro.
    """
    response = httpx2.get("https://api.example.com/items", timeout=30)
    response.raise_for_status()
    logger.info("Itens recebidos: %d", len(response.json()))
    path = f"/tmp/{dataset_id}/{table_id}.parquet"
    pl.DataFrame(response.json()).write_parquet(path)
    return path
```

```yaml
# pipelines/rj_secretaria__pipeline/prefect.yaml (trecho)  — Seções 8, 9 e 13
deployments:
  - name: rj-secretaria--pipeline--prod
    tags:
      - environment:prod
      - severity:medium
      - code_owner:seu_username
    schedules:
      - cron: "0 6 * * *"
        timezone: America/Sao_Paulo
        slug: daily
    work_pool:
      name: default-pool
```

```toml
# pipelines/rj_secretaria__pipeline/pyproject.toml  — Seção 10
[project]
name = "rj_secretaria__pipeline"
version = "1.0.0"
description = "Fetches the item list from the Example API and loads it into BigQuery for the secretaria's monitoring."
dependencies = ["prefect_rj_iplanrio", "polars>=2.0.0"]

[tool.uv.sources]
prefect_rj_iplanrio = { workspace = true }
```

Este exemplo não usa SQL. Se a sua pipeline executa SQL, crie `queries/` e leia a [Seção 7](#7-sql).

## Índice

- [Início rápido](#início-rápido)

1. [Filosofia](#1-filosofia)
2. [Arquitetura de código em três camadas](#2-arquitetura-de-código-em-três-camadas)
3. [Contrato de arquivos da pipeline](#3-contrato-de-arquivos-da-pipeline)
4. [Responsabilidades dos módulos](#4-responsabilidades-dos-módulos)
5. [Estilo Python](#5-estilo-python)
6. [Padrões do Prefect 3.0](#6-padrões-do-prefect-30)
7. [SQL](#7-sql)
8. [Agendamento](#8-agendamento)
9. [Convenções do `prefect.yaml`](#9-convenções-do-prefectyaml)
10. [`pyproject.toml`](#10-pyprojecttoml)
11. [`src/prefect_rj_iplanrio/` — código compartilhado do workspace](#11-srcprefect_rj_iplanrio--código-compartilhado-do-workspace)
12. [Higiene do repositório](#12-higiene-do-repositório)
13. [Tags de pipeline](#13-tags-de-pipeline)
14. [Testes](#14-testes)
15. [Checklist antes do pull request](#15-checklist-antes-do-pull-request)
16. [Glossário](#16-glossário)

Apêndice:

- [Apêndice A — Por que tags em vez de parâmetros](#apêndice-a--por-que-tags-em-vez-de-parâmetros)

## 1. Filosofia

Flows orquestram. Tasks encapsulam. Utils calculam. SQL é dado.

Qualquer pessoa deve conseguir abrir um diretório de pipeline e entender imediatamente sua estrutura, pois toda pipeline segue as mesmas convenções. Consistência vale mais do que otimização local.

## 2. Arquitetura de código em três camadas

O código neste repositório vive em um de três níveis. O nível determina onde uma função é escrita, não sua importância.

| Camada                          | Local                           | Regra                                                                                                                          |
| ------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| **Compartilhado de primitivos** | `packages/toolkit/` (workspace) | Primitivos reutilizáveis sem lógica de pipeline. Regras em [`packages/toolkit/STYLEGUIDE.md`](packages/toolkit/STYLEGUIDE.md). |
| **Compartilhado do repo**       | `src/prefect_rj_iplanrio/`      | Utilitários usados por **2 ou mais pipelines** neste repo sem lógica específica de pipeline.                                   |
| **Interno à pipeline**          | `pipelines/rj_x__y/utils.py`    | Lógica que pertence a exatamente uma pipeline.                                                                                 |

**Regra de promoção:** uma função sobe de `utils.py` para `src/prefect_rj_iplanrio/` quando uma segunda pipeline precisar dela. Nunca sobe por reutilização especulativa.

## 3. Contrato de arquivos da pipeline

### 3.1 Arquivos obrigatórios

Todo diretório de pipeline deve conter estes três arquivos obrigatórios.
Arquivos condicionais da Seção 3.2 podem ser adicionados quando necessários.

```
pipelines/rj_secretaria__pipeline/
├── flow.py          ← a função @flow
├── pyproject.toml   ← manifesto do pacote
└── Dockerfile       ← segue o template padrão, um por pipeline
```

### 3.2 Arquivos condicionais

Adicione apenas quando a necessidade existir concretamente:

```
├── tasks.py         ← quando o flow chama funções decoradas com @task
├── utils.py         ← quando as tasks delegam para helpers puros reutilizáveis
├── utils/           ← quando os helpers abrangem múltiplos domínios distintos
│   ├── api.py       ← nomeado pelo domínio, não pelo papel (nunca utils/tasks.py)
│   └── schemas.py
├── constants.py     ← quando constantes são compartilhadas entre 2+ módulos
├── queries/         ← quando a pipeline executa SQL
│   ├── fetch_data.sql
│   └── get_last_update.sql
└── tests/           ← quando utils.py tem lógica com ramificações ou parsing (Seção 14)
    └── test_utils.py
```

### 3.3 Proibidos nos diretórios de pipeline

O seguinte nunca deve ser commitado em um diretório de pipeline:

| Arquivo / padrão                                                       | Motivo                                                                                               |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `__init__.py`                                                          | Pacotes membros do workspace não precisam dele; sua presença causa confusão nos imports.             |
| `env.py`                                                               | Variáveis de ambiente são injetadas em runtime via Infisical ou o ambiente do container.             |
| `schedules.py`                                                         | Agendamentos vivem no `prefect.yaml`. Geradores Python de schedules são uma indireção desnecessária. |
| `build_prefect_yaml.py`                                                | Scripts avulsos ficam desatualizados e induzem ao erro.                                              |
| `scheduler*.yaml` (avulsos)                                            | Uma segunda fonte de verdade de agendamentos que diverge do `prefect.yaml`.                          |
| `test_*.py` na raiz da pipeline                                        | Scripts de teste ad-hoc não são reproduzíveis. Testes ficam em `tests/` e rodam com `pytest`.        |
| `FIX_SUMMARY.md`, `MIGRATION_NOTES.md`, `MIGRACAO_*.md`                | Artefatos de sessões de LLM. Delete antes de commitar.                                               |
| `.python-version`                                                      | A versão do Python é fixada na raiz do workspace no `pyproject.toml`.                                |
| `.DS_Store`                                                            | Metadados do macOS. Coberto pelo `.gitignore` — se aparecer, o gitignore falhou.                     |
| Arquivos de dados avulsos (`.json`, `.xml`, `.sql` fora de `queries/`) | Fixtures pertencem a `tests/fixtures/`; scripts DDL pertencem a um repositório de migrations.        |

## 4. Responsabilidades dos módulos

### 4.1 `flow.py`

**Contém:** exatamente uma função `@flow(log_prints=True)` cujo nome corresponde exatamente ao nome do diretório da pipeline.

**Papel:** orquestração pura — declara quais tasks rodam e em que ordem. Sem lógica de negócio, sem transformação de dados, sem I/O.

```python
# ✅ correto
"""Flow for rj_secretaria__pipeline."""

from prefect import flow

from pipelines.rj_secretaria__pipeline.tasks import (
    fetch_items_task,
    rename_flow_run_task,
    upload_task,
)


@flow(log_prints=True)
def rj_secretaria__pipeline(
    dataset_id: str = "brutos_example",
    table_id: str = "items",
) -> None:
    rename_flow_run_task(new_name=table_id)
    data_path = fetch_items_task(dataset_id=dataset_id, table_id=table_id)
    upload_task(data_path=data_path, dataset_id=dataset_id, table_id=table_id)
```

**Nunca contém:**

- O decorator `@task`
- `if __name__ == "__main__":`
- Lógica de negócio (busca de dados, transformação, chamadas de API)
- Strings SQL

### 4.2 `tasks.py`

**Contém:** exclusivamente funções decoradas com `@task`. Cada task é um wrapper fino que chama uma função de `utils.py` ou da biblioteca `toolkit`.

**Papel:** fronteira entre o engine de orquestração do Prefect e o Python puro. Tasks dão visibilidade ao Prefect — retries, rastreamento de estado, limites de concorrência — sem acoplar essa maquinaria à lógica em si.

```python
# ✅ correto — task delega para utils, permanece fina
from prefect import task
from pipelines.rj_secretaria__pipeline import utils


@task
def fetch_items_task(dataset_id: str, table_id: str) -> str:
    return utils.fetch_items(dataset_id=dataset_id, table_id=table_id)
```

```python
# ❌ errado — 80 linhas de lógica de API dentro de um @task
@task
def fetch_items_task(dataset_id: str, table_id: str) -> str:
    client = httpx2.Client(headers=...)
    response = client.get(...)
    # ... mais 70 linhas
```

**Nunca contém:** `@flow`, lógica de negócio com mais de ~20 linhas, `import`s sem relação com o Prefect ou com a chamada a `utils`.

### 4.3 `utils.py`

**Contém:** funções Python sem dependência do Prefect que realizam o trabalho real — chamadas de API, transformação de dados, I/O de arquivos e construção de queries.

**Papel:** o núcleo testável da pipeline. Por não terem imports do Prefect, essas funções podem ser chamadas diretamente no `pytest` sem subir um ambiente Prefect.

```python
# ✅ correto — sem Prefect, totalmente testável
import httpx2
import polars as pl


def fetch_items(dataset_id: str, table_id: str) -> str:
    response = httpx2.get("https://api.example.com/items")
    response.raise_for_status()
    df = pl.DataFrame(response.json())
    path = f"/tmp/{dataset_id}/{table_id}.parquet"
    df.write_parquet(path)
    return path
```

**Nunca contém:** `from prefect import ...`, `@task`, `@flow`, efeitos colaterais no momento do import do módulo.

### 4.4 `utils/<domain>.py`

Use um subdiretório `utils/` quando os helpers se dividem naturalmente em múltiplos domínios distintos e um único `utils.py` ficaria extenso demais. Nomeie cada módulo pelo domínio, não pelo papel.

```
utils/
├── api.py       ← helpers de client HTTP
├── schemas.py   ← modelos Pydantic / dataclasses
└── bq.py        ← helpers específicos do BigQuery
```

**`utils/tasks.py` é proibido.** Tasks são primitivos do Prefect, não utilitários. Se você se encontrar criando `utils/tasks.py`, está misturando duas responsabilidades. Coloque as tasks em `tasks.py` e a lógica no `utils/<domain>.py` adequado.

### 4.5 `constants.py`

**Contém:** constantes do módulo compartilhadas entre dois ou mais módulos da pipeline.

```python
# ✅ correto
from zoneinfo import ZoneInfo

SP_TZ = ZoneInfo("America/Sao_Paulo")
DEFAULT_PAGE_SIZE = 500
API_TIMEOUT = 120.0
```

**Nunca contém:** strings SQL, instâncias de dataclass, ou constantes usadas por apenas um módulo (defina-as lá).

### 4.6 `queries/`

**Contém:** um arquivo `.sql` por query lógica, nomeado descritivamente em `snake_case`. Parametrizado com placeholders `$variable` (veja a [Seção 7](#7-sql)).

```
queries/
├── get_last_update.sql
├── fetch_active_records.sql
└── count_pending.sql
```

## 5. Estilo Python

### 5.1 Type hints

Todas as assinaturas de função — parâmetros e tipo de retorno — devem ter type hints. Sem exceções.

```python
# ✅ correto
def fetch_items(dataset_id: str, table_id: str) -> str:
    ...

# ❌ errado — tipo de retorno ausente, tipos dos parâmetros ausentes
def fetch_items(dataset_id, table_id):
    ...
```

### 5.2 Sintaxe de tipos

Use a sintaxe de union do Python 3.10+ e os tipos genéricos embutidos. Não importe tipos de container do módulo `typing`. `TypedDict`, `Protocol` e `TYPE_CHECKING` continuam vindo de `typing`, pois não têm equivalente embutido.

```python
# ✅ correto
def process(
    items: list[str],
    config: dict[str, int],
    label: str | None = None,
) -> tuple[str, int]:
    ...

# ❌ errado
from typing import Dict, List, Optional, Tuple

def process(
    items: List[str],
    config: Dict[str, int],
    label: Optional[str] = None,
) -> Tuple[str, int]:
    ...
```

### 5.3 Imports

- Sem imports com wildcard (`from x import *`).
- Sem cabeçalho de encoding (`# -*- coding: utf-8 -*-`). UTF-8 é o padrão no Python 3; o cabeçalho é ruído e o hook de pre-commit o removerá.
- Imports explícitos apenas: liste cada nome que você usa.
- Sem `# noqa` nem `# pylint: disable=...` sem o id da regra e o motivo. Corrija o problema. Se o silêncio for inevitável, escreva `# noqa: EXXX  # motivo`.

```python
# ✅ correto
from toolkit.dbt import DbtRun
from toolkit.gcs import list_keys

# ❌ errado
from toolkit.gcs import *
```

### 5.4 Nomenclatura

Não use `_` como prefixo para indicar nomes internos/privados. Use fronteiras de módulo para controlar visibilidade — se algo não deve ser importado por outros módulos, mantenha-o no módulo onde pertence, não em um módulo globalmente visível com uma dica no nome.

Use `UPPER_SNAKE_CASE` apenas para constantes no nível do módulo. Dentro de funções, use `snake_case`.

```python
# ✅ correto — constante real no nível do módulo
DEFAULT_TMP_PATH = "/tmp/dump"


def dump(url: str) -> str:
    data_path = DEFAULT_TMP_PATH
    ...

# ❌ errado — variável local com nome de constante
def dump(url: str) -> str:
    DATA_PATH = "/tmp/dump"
    ...
```

### 5.5 Logging

`print()` é **proibido** em qualquer arquivo Python do repositório. Todo logging usa `get_logger` de `prefect_rj_iplanrio.logging`, o ponto de entrada único de logging e o lugar da configuração de logs comum a todo o workspace (formato, destino). Em uma `@task`, `get_run_logger()` do Prefect também é permitido quando a mensagem precisa aparecer na UI do Prefect.

```python
# ✅ correto
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


def fetch_items(dataset_id: str, table_id: str) -> str:
    logger.info("Buscando itens para dataset_id=%s", dataset_id)
    ...
    logger.warning("Nenhum item encontrado")
```

```python
# ❌ errado — import direto do stdlib sem a configuração do workspace
import logging
logger = logging.getLogger(__name__)

# ❌ errado — proibido
def fetch_items(dataset_id: str, table_id: str) -> str:
    print("Buscando itens")
```

### 5.6 Docstrings

Todas as funções devem ter uma docstring no formato reST. A linha de resumo usa modo imperativo e termina com ponto final. Como type hints já cobrem os tipos, as diretivas `:type:` e `:rtype:` são redundantes e devem ser omitidas.

```python
# ✅ correto
def load_query(caller_file: str, name: str, **params: object) -> str:
    """Carrega e renderiza um arquivo SQL do diretório ``queries/``.

    :param caller_file: Passe ``__file__`` do módulo chamador. Usado para
        resolver o diretório ``queries/`` relativo à pipeline.
    :param name: Nome do arquivo SQL sem a extensão ``.sql``.
    :param params: Variáveis substituídas no template via ``string.Template``.
    :returns: String SQL renderizada com todos os placeholders substituídos.
    :raises FileNotFoundError: Se ``queries/<name>.sql`` não existir.
    :raises KeyError: Se uma variável obrigatória estiver ausente de ``params``.
    """
    ...
```

```python
# ❌ errado — sem docstring
def load_query(caller_file: str, name: str, **params: object) -> str:
    sql_file = Path(caller_file).parent / "queries" / f"{name}.sql"
    return Template(sql_file.read_text(encoding="utf-8")).substitute(**params)
```

Regras:

- `:returns:` é omitido quando o tipo de retorno é `None`.
- `:raises:` lista **todas** as exceções que a função pode lançar intencionalmente.
- Uma função que só delega para outra (ex.: tasks finas) pode ter apenas a linha de resumo se não houver comportamento adicional a documentar.

### 5.7 Parâmetros em excesso

Funções em `utils.py` com mais de 5 parâmetros que formem um grupo coeso devem agrupar esses parâmetros em um `dataclass` ou `TypedDict`. A regra não se aplica a `@flow` e `@task`, cujos parâmetros precisam ser primitivos JSON-serializáveis para o Prefect.

Prefira `dataclass(frozen=True)` para configs imutáveis com defaults. Use `TypedDict` quando o caller precisa construir o dict diretamente e passá-lo com `**`.

```python
# ❌ errado — mais de 5 parâmetros sem agrupamento
def fetch_records(
    project: str,
    dataset_id: str,
    table_id: str,
    environment: str,
    page_size: int = 500,
    timeout: int = 30,
) -> pl.DataFrame:
    ...
```

```python
# ✅ correto — dataclass para config imutável com defaults
from dataclasses import dataclass


@dataclass(frozen=True)
class FetchConfig:
    project: str
    dataset_id: str
    table_id: str
    environment: str
    page_size: int = 500
    timeout: int = 30


def fetch_records(config: FetchConfig) -> pl.DataFrame:
    ...
```

```python
# ✅ TypedDict — quando o caller monta o dict e passa com **params
from typing import TypedDict, Unpack


class FetchParams(TypedDict):
    project: str
    dataset_id: str
    table_id: str
    environment: str
    page_size: int
    timeout: int


def fetch_records(**params: Unpack[FetchParams]) -> pl.DataFrame:
    ...
```

### 5.8 Dados tabulares

Use **Polars** para DataFrames e **DuckDB** para SQL sobre arquivos Parquet ou dados locais. `pandas` é proibido. Parquet é o formato intermediário padrão. CSV só é aceito quando a API de destino exigir.

**Eager (`pl.DataFrame`)** quando os dados cabem em memória e o processamento é em lote. **Lazy (`pl.LazyFrame`)** quando a fonte é um arquivo Parquet e a transformação pode ser otimizada pelo motor: use `scan_parquet` e `sink_parquet`.

```python
# ✅ correto — eager para um lote em memória
import polars as pl


def batch_to_frame(batch: list[list], columns: list[str]) -> pl.DataFrame:
    """Converte um lote de linhas em um DataFrame.

    :param batch: Lista de linhas, cada uma como lista de valores.
    :param columns: Nomes das colunas na mesma ordem dos valores.
    :returns: DataFrame com os dados do lote.
    """
    return pl.DataFrame(dict(zip(columns, zip(*batch, strict=True), strict=True)))
```

```python
# ✅ correto — lazy para arquivo → arquivo, e o Executor do toolkit para SQL no DuckDB
from pathlib import Path
from typing import TypedDict

import polars as pl
from psycopg.sql import Literal
from toolkit.executor.dataset import Dataset
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.executor import Executor


class StationTotal(TypedDict):
    """Uma linha do resultado de ``totals_by_station``."""

    station_id: str
    mm: float


def normalize_parquet(source: Path, dest: Path) -> None:
    """Normaliza os nomes das colunas de um Parquet.

    :param source: Parquet de entrada.
    :param dest: Parquet de saída.
    """
    lf = pl.scan_parquet(source)
    lf.rename({col: col.lower().replace(" ", "_") for col in lf.collect_schema().names()}).sink_parquet(dest)


async def totals_by_station(source: Path) -> Dataset[StationTotal]:
    """Soma os valores por estação com SQL do DuckDB.

    :param source: Parquet com as colunas ``station_id`` e ``mm``.
    :returns: Um dataset do Polars com uma linha por estação.
    """
    async with DuckDBEngine.connect() as engine:
        executor = Executor(engine, Path(__file__).parent / "queries")
        return await executor.query(
            "totals_by_station", {"source": Literal(str(source))}, expect=StationTotal
        )
```

```sql
-- queries/totals_by_station.sql
SELECT station_id, sum(mm) AS mm
FROM read_parquet({{ source }})
GROUP BY station_id
```

Uma pipeline nunca importa `duckdb`: o SQL que roda no DuckDB passa pelo `Executor` do toolkit (Seção 7.6).

Para extrair de uma API, escreva a paginação e as repetições na task (`@task(retries=3, retry_delay_seconds=30)`), grave cada página em um arquivo JSON Lines e entregue os arquivos ao toolkit com `stage_scan` ou `stage_files_task`. O Prefect cuida da ordem, das repetições e do estado; o `Executor` cuida do SQL.

```python
# ❌ errado — pandas e CSV intermediário
import pandas as pd

df = pd.read_csv(path)
df.to_csv(output_path, index=False)
```

### 5.9 Exceções

Não levante `BaseException`. Use o tipo de exceção mais específico ou crie uma subclasse.

```python
# ❌ errado
raise BaseException("Data need to be a polars DataFrame")

# ✅ correto
raise TypeError(f"Esperado pl.DataFrame, recebido {type(data).__name__}.")
```

### 5.10 Validação

Valide na borda, onde o dado entra. Duas ferramentas, cada uma com um papel:

| Camada                     | Ferramenta                  | Quando usar                                |
| -------------------------- | --------------------------- | ------------------------------------------ |
| Estrutura de DataFrame     | Polars nativo (`pl.Schema`) | Dado externo: banco, GCS, API              |
| Parâmetros e configurações | Pydantic v2 `BaseModel`     | Parâmetros de `@flow` e configs de conexão |

Declare o schema esperado ao ler dado externo. O Polars levanta `SchemaError` na hora, perto da origem. Use `@field_validator` do Pydantic v2. `@validator` (v1) é proibido.

```python
# ✅ correto — schema declarado na borda
import polars as pl
from pathlib import Path

ITEMS_SCHEMA = pl.Schema({"item_id": pl.String, "value": pl.Float64})


def read_items(path: Path) -> pl.DataFrame:
    """Lê os itens do Parquet e valida o schema.

    :param path: Caminho do Parquet.
    :returns: DataFrame com o schema de itens.
    :raises polars.exceptions.SchemaError: Se o arquivo não respeitar o schema.
    """
    return pl.read_parquet(path, schema=ITEMS_SCHEMA)
```

```python
# ✅ correto — Pydantic v2
from pydantic import BaseModel, field_validator


class UploadConfig(BaseModel):
    """Configuração de upload para o BigQuery."""

    dataset_id: str
    dump_mode: str

    @field_validator("dump_mode")
    @classmethod
    def validate_dump_mode(cls, value: str) -> str:
        """Aceita apenas ``append`` ou ``overwrite``."""
        if value not in {"append", "overwrite"}:
            raise ValueError(f"dump_mode inválido: {value!r}")
        return value
```

Não faça isto:

- I/O dentro de um validador (consulta ao BigQuery, leitura do GCS, chamada de API). O validador verifica estrutura e formato. O I/O fica na função que usa o dado.
- `assert` fora de testes para validar dado.
- Revalidar um dado que já passou pela borda.

## 6. Padrões do Prefect 3.0

### 6.1 Declaração de flow

```python
@flow(log_prints=True)
def rj_secretaria__pipeline(param: str = "default") -> None:
    ...
```

- `log_prints=True` é sempre definido como proteção contra chamadas `print()` acidentais. Todo logging intencional deve usar o módulo `logging` (veja [§5.5](#55-logging)).
- O nome da função deve corresponder **exatamente** ao nome do diretório da pipeline. É isso que o Prefect usa para identificar o flow nos deployments.
- O tipo de retorno é quase sempre `None` — flows produzem efeitos colaterais por natureza.

### 6.2 Declaração de task

```python
from prefect import task


@task
def fetch_data_task(url: str, timeout: int = 30) -> pl.DataFrame:
    return utils.fetch_data(url=url, timeout=timeout)
```

- Tasks ficam apenas em `tasks.py`.
- Cada task faz uma coisa.
- Type hints completos, incluindo o tipo de retorno.

### 6.3 Expressando a ordem das tasks — data flow em vez de `wait_for`

No Prefect 3, o grafo de execução é inferido a partir das dependências de dados. Quando a task B recebe o valor de retorno da task A como argumento, o Prefect sabe que A deve terminar antes de B começar. Esta é a forma correta de expressar ordenação.

`wait_for` é uma saída de emergência. Use-o apenas no caso raro em que a task B deve rodar após a task A, mas não consome o valor de retorno de A, e o Prefect não consegue inferir a dependência pelo grafo de chamadas.

```python
# ✅ correto — ordenação expressa por data flow
@flow(log_prints=True)
def rj_secretaria__pipeline(dataset_id: str, table_id: str) -> None:
    rename_flow_run_task(new_name=table_id)
    prepare_workdir_task()                                   # sequencial, sem output necessário
    last_update = get_last_update_task(dataset_id=dataset_id, table_id=table_id)
    data_path = fetch_history_task(last_update=last_update)  # dependência de dados — Prefect infere a ordem
    upload_task(data_path=data_path, dataset_id=dataset_id, table_id=table_id)
```

```python
# ❌ errado — wait_for usado onde existe uma dependência de dados
prepare_workdir_task(wait_for=[rename_task])
path = download_task(url=url, wait_for=[prepare_task])
```

```python
# ✅ wait_for aceitável — task de efeito colateral sem valor de retorno significativo
workdir = prepare_workdir_task()
path = download_task(url=url, wait_for=[workdir])  # apenas se download_task não usa o output de prepare
```

**Resumo:** se a task downstream puder aceitar o valor de retorno da task upstream como argumento, faça isso. Use `wait_for` apenas quando não há nenhuma relação de dados.

### 6.4 Padrões proibidos

| Padrão                                    | Por que é proibido                                                                                                                                  |
| ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@task` dentro de `flow.py`               | Mistura orquestração e definição de task em um único arquivo; torna as tasks invisíveis para consumidores de `tasks.py`.                            |
| `@flow` dentro de `tasks.py`              | Viola a responsabilidade única; subflows pertencem a arquivos `flow.py` separados, se necessário.                                                   |
| `utils/tasks.py`                          | Tasks não são utilitários. O nome é uma contradição e engana os leitores sobre onde encontrar as definições de task.                                |
| `if __name__ == "__main__":` em `flow.py` | Flows são executados via deployments do Prefect ou `prefect flow run`, não como scripts. Esse padrão incentiva contornar o mecanismo de deployment. |

## 7. SQL

### 7.1 A regra

Nenhuma string SQL em arquivos Python ou em arquivos YAML. Todo SQL vive em arquivos `.sql` dentro de `queries/`.

**Justificativa:**

- SQL embutido em strings Python é invisível para linters e editores de SQL, não pode ser testado isoladamente e é mais difícil de revisar em pull requests.
- As f-strings tornam a query dinâmica de um modo que não pode ser analisado estaticamente.
- `str.format()` colide com a sintaxe do BigQuery: `STRUCT<field STRING>` e `UNNEST([{}])` contêm o caractere `{`, que `str.format()` trata como placeholder.
- SQL embutido como parâmetro de schedule no `prefect.yaml` produz arquivos com centenas de linhas de SQL escapado em YAML, ilegíveis para revisão e invisíveis para qualquer linter de SQL.

### 7.2 Sintaxe de template

Use `string.Template` da biblioteca padrão do Python. Os placeholders usam a sintaxe `$variable` ou `${variable}`. O caractere `$` pode ter significado próprio em alguns dialetos. Para um `$` literal em `string.Template`, use `$$` e valide o SQL renderizado.

```sql
-- queries/get_last_update.sql
SELECT max(last_update) AS last_update
FROM `$project.${dataset_id}_staging.$table_id`
WHERE environment = '$environment'
  AND last_update IS NOT NULL
  AND last_update != 'None'
```

Use `$variable` para nomes simples. Use `${variable}` quando o placeholder estiver imediatamente adjacente a outros caracteres (ex.: `${dataset_id}_staging`).

### 7.3 Padrão de carregamento

Importe `load_query` do pacote compartilhado do workspace. Passe `__file__` do módulo chamador para que o caminho seja resolvido relativo à pipeline, independentemente do diretório de trabalho:

```python
# tasks.py
from prefect_rj_iplanrio.sql import load_query

query = load_query(
    __file__,
    "get_last_update",
    project="my-project",
    dataset_id=dataset_id,
    table_id=table_id,
    environment=environment,
)
```

`.substitute()` levanta `FileNotFoundError` se o arquivo `.sql` estiver ausente e `KeyError` se um `$variable` obrigatório estiver faltando — ambas são falhas explícitas que surgem imediatamente.

### 7.4 Substituição de fragmentos

Quando parte da estrutura SQL é computada em runtime (ex.: uma cláusula `WHERE` dinâmica ou um payload `UNNEST`), o fragmento computado ainda é substituído via `string.Template`. O arquivo `.sql` guarda o esqueleto; Python computa apenas as partes variáveis.

```sql
-- queries/cluster_alerts.sql
WITH alerts AS (
    SELECT *
    FROM UNNEST([$structs])
),
clustered AS (
    SELECT
        *,
        ST_CLUSTERDBSCAN(
            ST_GEOGPOINT(longitude, latitude),
            $radius_meters,
            1
        ) OVER (PARTITION BY alert_type) AS cluster_id
    FROM alerts
)
SELECT
    alert_type,
    cluster_id,
    ARRAY_AGG(alert_id) AS alert_ids,
    COUNT(*)            AS alert_count
FROM clustered
WHERE cluster_id IS NOT NULL
GROUP BY alert_type, cluster_id
```

```python
from prefect_rj_iplanrio.sql import load_query

structs = ", ".join(build_struct(a) for a in alerts)
query = load_query(__file__, "cluster_alerts", structs=structs, radius_meters=500)
```

O arquivo `.sql` permanece legível como SQL puro.

### 7.5 Parâmetros `query` em deployments

Flows que recebem SQL como parâmetro de deployment devem usar um parâmetro estruturado `query` com as chaves `name` e `replacements`. O `prefect.yaml` passa apenas o nome do arquivo `.sql` e os valores de substituição — nunca o conteúdo SQL.

**Cadeia completa:**

```yaml
# ✅ prefect.yaml — name é o arquivo .sql em queries/; replacements são os $placeholders
parameters:
  query:
    name: get_eligible_contacts
    replacements:
      dataset_id: brutos_example
      environment: production
```

```sql
-- ✅ queries/get_eligible_contacts.sql
SELECT *
FROM `my-project.$dataset_id.contacts`
WHERE environment = '$environment'
```

```python
# ✅ flow.py — recebe query do Prefect e repassa para a task
from prefect import flow

from pipelines.rj_secretaria__pipeline.tasks import QueryParam, fetch_data_task


@flow(log_prints=True)
def rj_secretaria__pipeline(query: QueryParam) -> None:
    fetch_data_task(query=query)
```

```python
# ✅ tasks.py
from typing import TypedDict

import polars as pl
from prefect import task
from prefect_rj_iplanrio.sql import load_query


class QueryParam(TypedDict):
    name: str
    replacements: dict[str, object]


@task
def fetch_data_task(query: QueryParam) -> pl.DataFrame:
    sql = load_query(__file__, query["name"], **query["replacements"])
    ...
```

**Pipelines de dump** (banco para BigQuery) seguem a mesma regra: o template usa `query: {name: extract_data, replacements: {}}` e o SQL fica em `queries/extract_data.sql`.

**Contrato:** as chaves de `replacements` devem corresponder exatamente aos placeholders `$variable` no arquivo `.sql`. `load_query` usa `string.Template.substitute()`, que levanta `KeyError` se um placeholder obrigatório estiver ausente — falha explícita e imediata.

Cada variante que exija SQL diferente recebe um arquivo `.sql` próprio em `queries/`. O diretório `queries/` é a única fonte de verdade do SQL da pipeline.

### 7.6 SQL que roda no DuckDB

O SQL que roda no DuckDB vive em `queries/*.sql` como template Jinja, e o `Executor` do toolkit o renderiza e o executa: `Executor(client, Path(__file__).parent / "queries")`. Valores entram como `Identifier()` e `Literal()` de `psycopg.sql`, não por `string.Template`. A limpeza que o DuckDB não faz é trabalho da task da pipeline. As Seções 7.2 a 7.5 valem para o SQL do BigQuery, que continua com `load_query`. Veja a Seção 5.1 do [guia do toolkit](packages/toolkit/STYLEGUIDE.md).

### 7.7 O que nunca fazer

```python
# ❌ SQL em f-string — não testável, não pesquisável
query = f"""
    SELECT max(last_update)
    FROM `my-project.{dataset_id}_staging.{table_id}`
    WHERE environment = '{environment}'
"""

# ❌ str.format() — colide com a sintaxe STRUCT<> do BigQuery
query = """
    SELECT STRUCT<name STRING, value INT64>(name, value)
    FROM `{project}.{dataset_id}`
""".format(project=project, dataset_id=dataset_id)

# ❌ String SQL em constants.py
FETCH_QUERY = """
    SELECT * FROM database.table;
"""
```

## 8. Agendamento

### 8.1 Regra

Agendamentos vivem exclusivamente no `prefect.yaml` sob a chave `schedules:` do deployment correspondente. Há uma única fonte de verdade.

### 8.2 Formato

```yaml
deployments:
  - name: rj-secretaria--pipeline--prod
    schedules:
      - cron: "0 6 * * 1-5" # dias úteis às 06:00
        timezone: America/Sao_Paulo
        slug: weekday-morning-run
      - interval: 3600 # a cada hora
        anchor_date: "2024-01-01T00:00:00"
        timezone: America/Sao_Paulo
        slug: hourly-run
        parameters:
          table_id: some_table
```

Sempre defina `timezone: America/Sao_Paulo`, exceto se o schedule for explicitamente baseado em UTC com razão documentada.

### 8.3 O que é proibido

| Artefato                    | Motivo                                                                                                                   |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `schedules.py`              | Gera config que precisa ser copiada para outro lugar — indireção extra, sempre com risco de divergência.                 |
| `build_prefect_yaml.py`     | Script que regenera o `prefect.yaml` — o arquivo gerado vira a fonte de verdade, mas o script é o que é mantido de fato. |
| `scheduler*.yaml` (avulsos) | Um segundo equivalente ao `prefect.yaml` que o Prefect não lê.                                                           |

## 9. Convenções do `prefect.yaml`

### 9.1 Nomenclatura de deployments

```
rj-<secretaria>--<pipeline>--staging
rj-<secretaria>--<pipeline>--prod
```

Use letras minúsculas e hífens apenas. O `--` duplo separa segmentos lógicos.

### 9.2 Work pool e secrets

O work pool depende do acesso de rede da pipeline: `default-pool` para o trabalho geral e `onprem-pool` para sistemas internos que exigem VPN (veja o README). Staging e prod de uma mesma pipeline usam o mesmo pool. Exemplo com `onprem-pool`:

```yaml
work_pool:
  name: onprem-pool
  work_queue_name: default
  job_variables:
    image: "{{ build-image.image_name }}:{{ build-image.tag }}"
    command: uv run --package rj_secretaria__pipeline -- prefect flow-run execute
    secretName: prefect-jobs-secrets-staging # staging
    image_pull_policy: Always
```

```yaml
secretName: prefect-jobs-secrets # prod
```

### 9.3 Staging vs. prod

Staging não tem a chave `schedules:`. Prod tem agendamentos. Staging usa `secretName: prefect-jobs-secrets-staging`. Prod usa `secretName: prefect-jobs-secrets`.

## 10. `pyproject.toml`

### 10.1 `description`

Uma frase que diga a um leitor — que nunca viu a pipeline antes — o que ela faz e por que existe.

```toml
# ✅ correto
description = "Fetches the item list from the Example API and loads it into BigQuery for the secretaria's monitoring."

# ❌ errado — texto gerado automaticamente sem informação
description = "Pipeline items da secretaria example"

# ❌ errado — padrão do cookiecutter não substituído
description = "TODO: replace with one sentence describing what this pipeline does and why."
```

### 10.2 `version`

Sempre `1.0.0`. O versionamento é tratado no nível do workspace via git tags e pela tag da imagem Docker construída a partir do hash do commit. Versões individuais de pacotes de pipeline não têm significado em um monorepo.

### 10.3 Dependências

Use limite inferior (`>=`) por padrão. Use `==` apenas com um comentário que explique o motivo (incompatibilidade conhecida, correção de segurança). O `uv.lock` garante a reprodutibilidade. A pipeline declara apenas os pacotes que importa e não repete pacotes que `prefect_rj_iplanrio` já fornece. Não adicione `pandas` (Seção 5.8).

```toml
# ✅ correto
dependencies = [
    "prefect_rj_iplanrio",
    "polars>=2.0.0",
    "lxml==6.1.1", # 6.2 quebra o parser HTML de x
]

# ❌ errado — pin exato sem motivo de pacote que o workspace já fornece
dependencies = ["prefect==3.4.9"]
```

## 11. `src/prefect_rj_iplanrio/` — código compartilhado do workspace

### 11.1 Critérios de promoção

Uma função é promovida do `utils.py` de uma pipeline para `src/prefect_rj_iplanrio/` quando **ambas** as condições forem verdadeiras:

1. Duas ou mais pipelines precisam dela.
2. Não contém lógica específica de nenhuma pipeline.

Promoção especulativa ("isso pode ser útil em outro lugar") não é motivo. Copie uma vez; promova no segundo uso.

### 11.2 API pública atual

| Módulo                        | Símbolo                                   | Propósito                                                                                             |
| ----------------------------- | ----------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `prefect_rj_iplanrio.logging` | `get_logger(name)`                        | Retorna o logger do módulo, pelo ponto de entrada único de logging. Passe `__name__` como argumento.  |
| `prefect_rj_iplanrio.sql`     | `load_query(caller_file, name, **params)` | Carrega e renderiza um arquivo `.sql` do diretório `queries/` relativo ao chamador. Passe `__file__`. |

### 11.3 Antes de escrever um helper

Procure primeiro no [README do toolkit](packages/toolkit/README.md).

## 12. Higiene do repositório

### 12.1 Requisitos do `.gitignore`

O `.gitignore` raiz deve cobrir:

```gitignore
# macOS
.DS_Store

# Python
__pycache__/
*.py[cod]
.ruff_cache/

# Ambientes
.env
.venv

# Pins de versão do Python por pipeline (gerenciados apenas na raiz do workspace)
pipelines/**/.python-version

# Diretórios egg-info gerados dentro dos pacotes de pipeline
pipelines/**/*.egg-info/
```

### 12.2 O que nunca deve ser commitado

Veja a [Seção 3.3](#33-proibidos-nos-diretórios-de-pipeline) para a lista completa. A versão resumida: se um arquivo foi criado para depurar algo, corrigir uma migração ou guardar dados temporários durante o desenvolvimento, ele não pertence ao repositório.

Também não entram no repositório:

- Código comentado em bloco. Use `git log` para recuperar código removido.
- Arquivos `.py` completamente vazios.

Na dúvida, pergunte: "Um novo membro do time lendo este arquivo entenderia por que ele existe e o que fazer com ele?" Se a resposta for não, apague-o.

## 13. Tags de pipeline

### 13.1 O que são tags

Toda pipeline **deve ter tags de metadados** definidas no `prefect.yaml` para indicar:

- **Code owner**: GitHub username do desenvolvedor responsável
- **Severidade**: Criticidade da pipeline (`low`, `medium`, `high`, `critical`)
- **Ambiente**: Estágio de execução (`staging`, `prod`)

Tags são metadados da infraestrutura do Prefect, não parâmetros da pipeline. Elas são propagadas a todos os logs estruturados e usadas pelo sistema de observabilidade para atribuição de alertas e priorização de incidentes.

### 13.2 Como definir tags

Tags são definidas na seção `tags:` de cada deployment, usando o padrão semântico `chave:valor`:

```yaml
deployments:
  - name: rj-secretaria--pipeline--staging
    tags:
      - environment:staging
      - severity:medium
      - code_owner:seu_username
    version: "{{ build-image.tag }}"
    entrypoint: pipelines/rj_secretaria__pipeline/flow.py:rj_secretaria__pipeline
    work_pool:
      name: onprem-pool
      work_queue_name: default
      job_variables:
        image: "{{ build-image.image_name }}:{{ build-image.tag }}"
        command: uv run --package rj_secretaria__pipeline -- prefect flow-run execute
        secretName: prefect-jobs-secrets-staging
        image_pull_policy: Always

  - name: rj-secretaria--pipeline--prod
    tags:
      - environment:prod
      - severity:high
      - code_owner:seu_username
    version: "{{ get-commit-hash.stdout }}"
    entrypoint: pipelines/rj_secretaria__pipeline/flow.py:rj_secretaria__pipeline
    work_pool:
      name: onprem-pool
      work_queue_name: default
      job_variables:
        image: "{{ build-image.image_name }}:{{ build-image.tag }}"
        command: uv run --package rj_secretaria__pipeline -- prefect flow-run execute
        secretName: prefect-jobs-secrets
        image_pull_policy: Always
```

**Padrão de tags:** Use `chave:valor` para facilitar filtros e buscas no Prefect Cloud.

### 13.3 Valores válidos

| Tag           | Valores válidos                     | Descrição                                                |
| ------------- | ----------------------------------- | -------------------------------------------------------- |
| `environment` | `staging`, `prod`                   | Estágio de execução — obrigatório em todo deployment     |
| `severity`    | `low`, `medium`, `high`, `critical` | Criticidade da pipeline — obrigatório em todo deployment |
| `code_owner`  | GitHub username                     | Responsável pelo código — obrigatório em todo deployment |

### 13.4 Por que tags e não parâmetros

Tags são metadados da infraestrutura. Parâmetros são configuração do flow. Veja a comparação no [Apêndice A](#apêndice-a--por-que-tags-em-vez-de-parâmetros).

### 13.5 Verificação

A revisão do PR verifica as tags sempre que o `prefect.yaml` muda. Ainda não existe verificação automática no CI. Cada deployment deve incluir:

- `environment:staging` ou `environment:prod`
- `severity` com um dos 4 valores válidos
- `code_owner` com um GitHub username válido

Quem revisa o PR deve recusar um deployment sem tags.

## 14. Testes

### 14.1 O que testar

Lógica com ramificações ou parsing em `utils.py` **deve** ter teste. Tasks finas e flows não precisam de teste: não têm lógica própria (Seções 4.1 e 4.2).

### 14.2 Estrutura

`tests/` fica dentro do diretório da pipeline e espelha os módulos: `utils.py` é testado por `tests/test_utils.py`. Fixtures ficam em `tests/fixtures/`. `pytest` é dependência de desenvolvimento da pipeline.

```
pipelines/rj_secretaria__pipeline/
├── utils.py
└── tests/
    ├── test_utils.py
    └── fixtures/
        └── response.json
```

### 14.3 Estilo

Use `pytest` e `@pytest.mark.parametrize`, com `ids` que descrevem o cenário. Os testes não usam rede, credenciais nem o runtime do Prefect. Use mocks só para verificar um contrato de interface (por exemplo, que uma função recebeu a query correta), nunca para testar lógica de negócio. Para funções puras de transformação, `hypothesis` é permitido para verificar propriedades como idempotência e preservação do número de linhas.

```python
import pytest

from pipelines.rj_secretaria__pipeline.utils import parse_value


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("1,5", 1.5), ("", None)],
    ids=["decimal_comma", "empty"],
)
def test_parse_value(raw: str, expected: float | None) -> None:
    assert parse_value(raw) == expected
```

## 15. Checklist antes do pull request

Marque cada item antes de pedir revisão.

- [ ] O diretório se chama `rj_<secretaria>__<pipeline>` e tem `flow.py`, `pyproject.toml` e `Dockerfile` ([Seção 3](#3-contrato-de-arquivos-da-pipeline)).
- [ ] Não há `__init__.py`, `env.py`, `schedules.py`, `.python-version` nem arquivos avulsos ([Seção 3.3](#33-proibidos-nos-diretórios-de-pipeline)).
- [ ] O `flow.py` só orquestra. A função `@flow` tem o mesmo nome do diretório ([Seção 4.1](#41-flowpy)).
- [ ] As tasks são wrappers finos e a lógica fica em `utils.py` ([Seções 4.2 e 4.3](#42-taskspy)).
- [ ] Não há `print()`. O logging usa `get_logger` ([Seção 5.5](#55-logging)).
- [ ] Toda função tem type hints e docstring reST ([Seções 5.1 e 5.6](#51-type-hints)).
- [ ] O código novo usa Polars e DuckDB, sem pandas ([Seção 5.8](#58-dados-tabulares)).
- [ ] Não há SQL em Python nem em YAML. O SQL está em `queries/*.sql` ([Seção 7](#7-sql)).
- [ ] O agendamento está só no `prefect.yaml`, em `America/Sao_Paulo`. O staging não tem `schedules` ([Seções 8 e 9](#8-agendamento)).
- [ ] Cada deployment tem as tags `environment`, `severity` e `code_owner` ([Seção 13](#13-tags-de-pipeline)).
- [ ] O `pyproject.toml` tem `version = "1.0.0"`, um `description` real e dependências com `>=` ([Seção 10](#10-pyprojecttoml)).
- [ ] Existe teste para a lógica com ramificações ou parsing de `utils.py` ([Seção 14](#14-testes)).

### Quem verifica cada regra

| Regra                                    | Verificação hoje                                                     |
| ---------------------------------------- | -------------------------------------------------------------------- |
| Estrutura de arquivos, estilo, SQL, tags | Revisão do PR (ainda não há verificação automática)                  |
| Formatação e lint (`ruff`)               | Configurado no `pyproject.toml`; o hook de pre-commit está desligado |
| Build, deploy dos flows, análise SAST    | CI do GitHub (`.github/workflows/`)                                  |

## 16. Glossário

| Termo                          | Significado                                                                                                                                                                          |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| flow                           | A função `@flow` do Prefect. Define quais tasks rodam e em que ordem.                                                                                                                |
| task                           | A função `@task` do Prefect. Tem retries e estado visíveis na UI.                                                                                                                    |
| deployment                     | Um flow registrado no Prefect com agendamento, parâmetros e infraestrutura. Fica no `prefect.yaml` ([Seção 9](#9-convenções-do-prefectyaml)).                                        |
| work pool                      | Onde o Prefect executa o deployment: `default-pool` (geral) ou `onprem-pool` (acesso interno por VPN).                                                                               |
| work queue                     | Fila dentro do work pool. Neste repositório sempre `default`.                                                                                                                        |
| `secretName`                   | Nome do Kubernetes Secret montado no container. Guarda credenciais como variáveis de ambiente.                                                                                       |
| Infisical                      | Serviço de segredos. As credenciais chegam ao código por variáveis de ambiente injetadas em runtime.                                                                                 |
| tag                            | Metadado `chave:valor` do deployment (`environment`, `severity`, `code_owner`) ([Seção 13](#13-tags-de-pipeline)).                                                                   |
| imagem / `Dockerfile`          | Cada pipeline roda em uma imagem Docker construída a partir da imagem base do repositório.                                                                                           |
| workspace e `--package`        | O repositório é um workspace do `uv`. `uv run --package <pipeline>` executa uma pipeline com as dependências dela.                                                                   |
| staging e prod                 | Os dois ambientes. Staging não tem agendamento. Prod tem.                                                                                                                            |
| promoção                       | Mover uma função de `utils.py` para `src/prefect_rj_iplanrio/` quando uma segunda pipeline precisa dela ([Seção 11](#11-srcprefect_rj_iplanrio--código-compartilhado-do-workspace)). |
| pipeline de dump               | Pipeline que copia dados de um banco para o BigQuery. O template do repositório cria esse tipo.                                                                                      |
| `{{ build-image.tag }}`        | Valor que o Prefect preenche no deploy com a tag da imagem construída. Vem do passo `build-image` do `prefect.yaml`.                                                                 |
| `{{ get-commit-hash.stdout }}` | Valor que o Prefect preenche no deploy com o hash curto do commit. Vem do passo `get-commit-hash`.                                                                                   |

## Apêndice A — Por que tags em vez de parâmetros

| Aspecto             | Tags                                             | Parâmetros                                  |
| ------------------- | ------------------------------------------------ | ------------------------------------------- |
| **Definição**       | Metadados imutáveis da infraestrutura            | Configuração mutável do flow                |
| **Escopo**          | Visíveis no Prefect Cloud; usado em filtros      | Passados como argumentos ao flow            |
| **Mutabilidade**    | Alteradas apenas no `prefect.yaml`               | Podem ser sobrescritas em tempo de execução |
| **Observabilidade** | Automaticamente propagadas aos logs estruturados | Requerem injeção manual no flow             |
| **Contaminação**    | O flow fica limpo; sem parâmetros "framework"    | O flow acumula parâmetros de infraestrutura |

Tags separam **infraestrutura** (Prefect) de **negócio** (parâmetros do flow).
