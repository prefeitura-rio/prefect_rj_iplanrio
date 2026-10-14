---
lang: pt-br
---

# Guia de Estilo

Regras para escrever, estruturar e manter pipelines neste monorepo. Cada regra declara o que é, por que existe e apresenta um exemplo concreto.

As regras de Python, dados e testes valem também para `packages/toolkit/`. As regras só do toolkit estão em [`packages/toolkit/STYLEGUIDE.md`](packages/toolkit/STYLEGUIDE.md).

## Início rápido

Vai criar uma pipeline nova? Siga estes cinco passos. Cada passo aponta para a seção com a regra completa.

1. **Crie a pipeline com o template.** Rode `uvx cookiecutter templates --output-dir=pipelines`. O template cria o diretório de uma pipeline de dump, com `flow.py`, `prefect.yaml`, `pyproject.toml` e `Dockerfile`. Para uma pipeline própria, troque o `flow.py` e acrescente `tasks.py`, `utils.py` e `queries/` quando precisar. O nome segue o padrão `rj_<secretaria>__<pipeline>` ([Seção 3](#3-contrato-de-arquivos-da-pipeline)).
2. **Descreva a pipeline.** Escreva o `description` no `pyproject.toml` ([Seção 10](#10-pyprojecttoml)).
3. **Escreva o código nos arquivos certos.** O flow só orquestra, a task é um wrapper fino e o trabalho real fica em `utils.py` ([Seção 4](#4-responsabilidades-dos-módulos)). O SQL fica em `queries/*.sql` ([Seção 7](#7-sql)). Antes de escrever um helper, procure no [README do toolkit](packages/toolkit/README.md).
4. **Configure o deploy.** No `prefect.yaml`, defina as tags, o agendamento da produção e o work pool ([Seções 8, 9 e 13](#8-agendamento)).
5. **Teste e abra o PR.** Escreva testes para a lógica de `utils.py` ([Seção 14](#14-testes)) e passe pelo [checklist](#15-checklist-antes-do-pull-request).

Existem dois pontos de partida:

- **Pipeline de dump (banco para BigQuery):** importe o fluxo `dump_database` de `utils.flows` no `flow.py` e ponha os parâmetros no `prefect.yaml` (veja o `README.md`).
- **Pipeline própria:** adicione `utils.py` com a lógica e, se precisar, mais tasks em `tasks.py`.

### Exemplo mínimo completo

Uma pipeline que baixa uma lista de itens de uma API e salva em Parquet. Os comentários indicam a seção que explica cada arquivo.

```python
# pipelines/rj_secretaria__pipeline/flow.py  — Seção 4.1
"""Flow for rj_secretaria__pipeline."""

from prefect import flow

from pipelines.rj_secretaria__pipeline.tasks import fetch_items_task


@flow(log_prints=True, flow_run_name="{table_id}")
def rj_secretaria__pipeline(
    dataset_id: str = "brutos_example",
    table_id: str = "items",
) -> None:
    fetch_items_task(dataset_id=dataset_id, table_id=table_id)
```

```python
# pipelines/rj_secretaria__pipeline/tasks.py  — Seção 4.2
"""Tasks for rj_secretaria__pipeline."""

from prefect import task

from pipelines.rj_secretaria__pipeline import utils


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
from prefect.logging import get_logger

logger = get_logger("toolkit")


def fetch_items(dataset_id: str, table_id: str) -> str:
    """Baixe os itens da API, salve em Parquet e devolva o caminho do arquivo.

    Levante `httpx2.HTTPStatusError` se a API responder com erro.
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
11. [`src/utils/` — código compartilhado do workspace](#11-srcutils--código-compartilhado-do-workspace)
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

### 1.1 Princípios

- Leia o código antes de escrever código.
- Prefira apagar a criar uma abstração ou uma camada de configuração.
- Faça mudanças pequenas e fáceis de revisar. Não misture trabalhos sem relação.
- Corrija a causa, não o sintoma.
- Mantenha uma única fonte de verdade para cada fato.
- Use a ferramenta idiomática de cada ecossistema.
- Siga um padrão que já existe antes de criar um novo.

## 2. Arquitetura de código em três camadas

O código neste repositório vive em um de três níveis. O nível determina onde uma função é escrita, não sua importância.

| Camada                          | Local                           | Regra                                                                                                                          |
| ------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| **Compartilhado de primitivos** | `packages/toolkit/` (workspace) | Primitivos reutilizáveis sem lógica de pipeline. Regras em [`packages/toolkit/STYLEGUIDE.md`](packages/toolkit/STYLEGUIDE.md). |
| **Compartilhado do repo**       | `src/utils/`                    | Utilitários usados por **2 ou mais pipelines** neste repo sem lógica específica de pipeline.                                   |
| **Interno à pipeline**          | `pipelines/rj_x__y/utils.py`    | Lógica que pertence a exatamente uma pipeline.                                                                                 |

**Regra de promoção:** uma função sobe de `utils.py` para `src/utils/` quando uma segunda pipeline precisar dela. Nunca sobe por reutilização especulativa.

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

from pipelines.rj_secretaria__pipeline.tasks import fetch_items_task, upload_task


@flow(log_prints=True, flow_run_name="{table_id}")
def rj_secretaria__pipeline(
    dataset_id: str = "brutos_example",
    table_id: str = "items",
) -> None:
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

**Contém:** um arquivo `.sql` por query lógica, nomeado descritivamente em `snake_case`. Cada arquivo é um modelo Jinja que o `Executor` renderiza (veja a [Seção 7](#7-sql)).

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
- Sem cabeçalho de encoding (`# -*- coding: utf-8 -*-`). UTF-8 é o padrão no Python 3, e o cabeçalho é ruído.
- Imports explícitos apenas: liste cada nome que você usa.
- Importe nomes, não módulos: `from asyncio import to_thread`, e não `import asyncio`. Se o nome colide com um nome local, dê um apelido (`from duckdb import connect as duckdb_connect`). As exceções são o `pytest` (`import pytest`, que o `ruff` exige pela regra `PT013`) e o módulo que um teste precisa para trocar um nome (`from toolkit import dbt`, e então `monkeypatch.setattr(dbt, ...)`).
- Sem `# noqa` nem `# pylint: disable=...` sem o id da regra e o motivo. Corrija o problema. Se o silêncio for inevitável, escreva `# noqa: EXXX  # motivo`.

```python
# ✅ correto
from asyncio import to_thread
from toolkit.dbt import DbtRun
from toolkit.gcs import list_keys

# ❌ errado
import asyncio
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

`print()` é **proibido** em qualquer arquivo Python do repositório. Todo logging usa `get_logger("toolkit")` de `prefect.logging`. Ele devolve o logger do workspace, `prefect.toolkit`, o mesmo de `toolkit.telemetry.logger`: tem a formatação padrão do Prefect, mostra INFO, aparece na UI do Prefect e, com a exportação do OpenTelemetry ligada, segue para o coletor. Um logger de `logging.getLogger(__name__)` fica fora da árvore `prefect`, então não tem nada disso e perde as mensagens INFO. Em uma `@task`, `get_run_logger()` do Prefect também é permitido quando a mensagem precisa aparecer na UI do Prefect.

```python
# ✅ correto
from prefect.logging import get_logger

logger = get_logger("toolkit")


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

Regras de conteúdo:

- Escreva frases de ciclo de vida que uma pessoa lê: `<Componente> <ação> <estado>: <contexto>`, como `Itens buscados: dataset_id=brutos_x rows=120`.
- Use `debug` para detalhe de sondagem, `info` para mudança normal de ciclo de vida, `warning` para nova tentativa ou trabalho opcional pulado, e `error` só antes de um erro terminal.
- Nunca registre credenciais, tokens, strings de conexão, corpos de requisição, valores de cache, valores de SQL nem query strings.
- Mantenha a saída de ferramentas externas (dbt, DuckDB) como ela vem. Não a interprete nem a reformate.

### 5.6 Docstrings

Toda função tem uma `docstring` em Markdown, sem `reST`: código com uma crase, sem `:param:`, `:returns:` nem `:type:`. Os type hints já dizem os tipos. O resumo é uma frase curta no imperativo, em uma linha, com ponto final e até 88 colunas. Escreva mais frases só se o contrato não aparecer no nome e nos tipos, após uma linha em branco. O erro que a função levanta entra na `docstring`. Uma função difícil de entender leva um exemplo em bloco `python` cercado, com o resultado num comentário `#` conferido ao rodar.

```python
# ✅ correto — o contrato cabe numa linha, e o erro vem depois
def fetch_items(dataset_id: str, table_id: str) -> str:
    """Baixe os itens da API, salve em Parquet e devolva o caminho do arquivo.

    Levante `httpx2.HTTPStatusError` se a API responder com erro.
    """
    ...
```

```python
# ✅ correto — uma função difícil leva um exemplo
def normalize_column(name: str) -> str:
    """Devolva o nome de coluna que o BigQuery aceita.

    ```python
    normalize_column("Código do Órgão")  # "codigo_do_orgao"
    ```
    """
    ...
```

```python
# ❌ errado — reST, e um resumo que repete o que os tipos dizem
def fetch_items(dataset_id: str, table_id: str) -> str:
    """Baixa os itens da API e salva em Parquet.

    :param dataset_id: Dataset de destino no BigQuery.
    :returns: Caminho do arquivo Parquet.
    """
    ...
```

Regras:

- Uma função que só delega para outra (ex.: tasks finas) pode ter apenas a linha de resumo.
- Uma classe leva um sintagma nominal ("Uma linha do resultado de `totals_by_station`."), e uma função com `@overload` não leva `docstring`.
- Sem o rótulo `Example:`. Um exemplo é um bloco `python` ou `sql` cercado.

### 5.7 Parâmetros em excesso

Funções em `utils.py` com mais de 5 parâmetros que formem um grupo coeso devem agrupar esses parâmetros em um `dataclass` ou `TypedDict`. Em `@flow` e `@task` a regra vale também, mas só com `TypedDict`, porque os parâmetros precisam ser JSON-serializáveis para o Prefect. O Prefect valida o dicionário e recusa uma chave que falta, um erro de digitação ou um tipo errado antes de rodar. Ponha cada grupo em `types.py`.

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
    """Converta um lote de linhas em um DataFrame, com os nomes das colunas dados."""
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
    """Uma linha do resultado de `totals_by_station`."""

    station_id: str
    mm: float


def normalize_parquet(source: Path, dest: Path) -> None:
    """Normalize os nomes das colunas de um Parquet e grave o resultado em `dest`."""
    lf = pl.scan_parquet(source)
    lf.rename({col: col.lower().replace(" ", "_") for col in lf.collect_schema().names()}).sink_parquet(dest)


async def totals_by_station(source: Path) -> Dataset[StationTotal]:
    """Some os valores de um Parquet com as colunas `station_id` e `mm`, por estação."""
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

Uma pipeline nunca importa `duckdb`: o SQL que roda no DuckDB passa pelo `Executor` do toolkit (Seção 7.2).

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
    """Leia os itens do Parquet no schema de itens.

    Levante `polars.exceptions.SchemaError` se o arquivo não respeitar o schema.
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
- Dê nome à run com `flow_run_name="{table_id}"` no `@flow`: o Prefect preenche o texto com os parâmetros do flow. Não renomeie a run com uma task.

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
@flow(log_prints=True, flow_run_name="{table_id}")
def rj_secretaria__pipeline(dataset_id: str, table_id: str) -> None:
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

Nenhuma string SQL em arquivos Python ou em arquivos YAML. Todo SQL vive em arquivos `.sql` dentro de `queries/`, como modelo Jinja que o `Executor` do toolkit renderiza e executa.

**Justificativa:**

- SQL embutido em strings Python é invisível para linters e editores de SQL, não pode ser testado isoladamente e é mais difícil de revisar em pull requests.
- As f-strings tornam a query dinâmica de um modo que não pode ser analisado estaticamente.
- `str.format()` colide com a sintaxe do BigQuery: `STRUCT<field STRING>` e `UNNEST([{}])` contêm o caractere `{`.
- SQL embutido como parâmetro de schedule no `prefect.yaml` produz arquivos com centenas de linhas de SQL escapado em YAML, ilegíveis para revisão.

**Exceção:** o parâmetro `read.query` do fluxo `dump_database` é a consulta nativa de um banco externo, escrita no dialeto dele, e vai no `prefect.yaml` da pipeline.

### 7.2 Modelos Jinja e o Executor

O `Executor` renderiza o modelo com `StrictUndefined` (uma variável ausente é um erro) e o roda no cliente. Valores entram como `Identifier()` e `Literal()` de `psycopg.sql`, nunca como texto no SQL, e valores de execução como parâmetros (`?`). As regras do modelo estão na [Seção 3.1 do guia do toolkit](packages/toolkit/STYLEGUIDE.md#31-modelos).

```sql
-- queries/last_update.sql
{#
{
  "kind": "template",
  "description": "Read the last update of a table.",
  "inputs": { "table": "Identifier of the table." }
}
#}
SELECT max(last_update) AS last_update FROM {{ table }}
```

```python
# tasks.py
from pathlib import Path

from psycopg.sql import Identifier

executor = Executor(client, Path(__file__).parent / "queries")
result = await executor.query("last_update", {"table": Identifier("src", "t")}, expect=LastUpdate)
```

Cada variante que exija SQL diferente recebe um arquivo `.sql` próprio em `queries/`. O diretório `queries/` é a única fonte de verdade do SQL da pipeline.

### 7.3 Operações comuns já prontas

O toolkit tem funções do `DuckDB` em toda conexão, sem importar nada: `normalize(coluna, separador, minúsculas)`, `clean_text(texto)`, `parse_date(texto)`, `parse_timestamp(texto)`, `ano_particao(texto)`, `mes_particao(texto)`, `data_particao(texto)`, `now_sao_paulo()`, `to_sao_paulo(instante)`, `is_cpf`, `normalize_cpf`, `is_cnpj`, `normalize_cnpj` e `is_phone`. Use-as no SQL, como em `clean_text(nome)` e `normalize(nome, '_', true)`. Os modelos prontos são `extract_window`, `select_source` e `fingerprint`. No `dump_database`, `normalize_values` e `transforms` aplicam essas funções dentro do `COPY` de cada janela. Para os nomes das colunas de uma tabela, use `Executor.normalize_columns(tabela, colunas, separador, minúsculas)`.

### 7.4 O que nunca fazer

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

## 11. `src/utils/` — código compartilhado do workspace

### 11.1 Critérios de promoção

Uma função é promovida do `utils.py` de uma pipeline para `src/utils/` quando **ambas** as condições forem verdadeiras:

1. Duas ou mais pipelines precisam dela.
2. Não contém lógica específica de nenhuma pipeline.

Promoção especulativa ("isso pode ser útil em outro lugar") não é motivo. Copie uma vez; promova no segundo uso.

### 11.2 API pública atual

| Módulo        | Símbolo                                                                                                                                                                                                                                                   | Propósito                                                                                                            |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `utils.tasks` | `dump_db_to_gcs`, `publish_table_in_bq`, `upload_files_to_gcs`, `dump_bq_to_gcs`, `dump_arcgis_to_gcs`, `dump_gsheet_to_gcs`, `write_sheet`, `request_json`, `write_records`, `read_bq`, `run_bq_sql`, `notify_discord`, `compact_delta_table`, `run_dbt` | Tasks compartilhadas sobre o toolkit. Veja o `README.md`.                                                            |
| `utils.flows` | `dump_database`, `dump_arcgis`, `dump_gsheet`, `load_files`, `ingest_api`, `compact_tables`, `dbt_run`                                                                                                                                                    | Fluxos: o dump de uma fonte para uma tabela `Delta` sem repetir dado, a captura de uma API, a compactação e o `dbt`. |

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

### 12.3 Commits

- Use Conventional Commits: `tipo(escopo): resumo no imperativo`, em minúsculas e sem ponto final. O escopo é opcional.
- Use os tipos `feat`, `fix`, `refactor`, `test`, `docs` e `chore` (CI, ferramentas, dependências e manutenção).
- Faça uma mudança lógica por commit. Junte os commits de correção (squash) antes do merge.
- Nunca faça commit nem push sem aprovação explícita. Prepare, resuma e espere.

```text
refactor(toolkit): move GCS operations to obstore
```

### 12.4 Documentação

- Escreva frases curtas, na voz ativa, com uma ideia por frase.
- Deixe cada assunto em um documento só. Aponte para o documento dono com um link, sem copiar o texto.
- Use exemplos genéricos, a não ser que um valor real seja necessário.
- Confira cada afirmação no código, nos templates, nos testes ou em um comando que você rodou.

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

- Ponha cada teste no nível mais baixo que prova o comportamento. Não repita a mesma verificação em um nível mais alto.
- Confira valores exatos. Não use `in` sobre texto, a não ser uma regex em texto gerado que seria frágil de comparar inteiro.
- Use o `hypothesis` só para invariantes sobre entradas geradas.
- Não teste o armazenamento de campos, valores padrão nem "nenhuma exceção foi levantada".
- Junte arquivos de teste pequenos que cobrem um módulo.

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
- [ ] Toda função tem type hints e `docstring` em Markdown, com resumo de uma linha ([Seções 5.1 e 5.6](#51-type-hints)).
- [ ] O código novo usa Polars e DuckDB, sem pandas ([Seção 5.8](#58-dados-tabulares)).
- [ ] Não há SQL em Python nem em YAML. O SQL está em `queries/*.sql` ([Seção 7](#7-sql)).
- [ ] O agendamento está só no `prefect.yaml`, em `America/Sao_Paulo`. O staging não tem `schedules` ([Seções 8 e 9](#8-agendamento)).
- [ ] Cada deployment tem as tags `environment`, `severity` e `code_owner` ([Seção 13](#13-tags-de-pipeline)).
- [ ] O `pyproject.toml` tem `version = "1.0.0"`, um `description` real e dependências com `>=` ([Seção 10](#10-pyprojecttoml)).
- [ ] Existe teste para a lógica com ramificações ou parsing de `utils.py` ([Seção 14](#14-testes)).

### Quem verifica cada regra

| Regra                                    | Verificação hoje                                                   |
| ---------------------------------------- | ------------------------------------------------------------------ |
| Estrutura de arquivos, estilo, SQL, tags | Revisão do PR (ainda não há verificação automática)                |
| Formatação e lint (`ruff`)               | Configurado no `pyproject.toml`; rode `ruff` à mão                 |
| Tipos (`basedpyright` e `pyrefly`)       | Configurados no `pyproject.toml`, em `src/` e `tests/`; rode à mão |
| Build, deploy dos flows, análise SAST    | CI do GitHub (`.github/workflows/`)                                |

## 16. Glossário

| Termo                          | Significado                                                                                                                                                            |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| flow                           | A função `@flow` do Prefect. Define quais tasks rodam e em que ordem.                                                                                                  |
| task                           | A função `@task` do Prefect. Tem retries e estado visíveis na UI.                                                                                                      |
| deployment                     | Um flow registrado no Prefect com agendamento, parâmetros e infraestrutura. Fica no `prefect.yaml` ([Seção 9](#9-convenções-do-prefectyaml)).                          |
| work pool                      | Onde o Prefect executa o deployment: `default-pool` (geral) ou `onprem-pool` (acesso interno por VPN).                                                                 |
| work queue                     | Fila dentro do work pool. Neste repositório sempre `default`.                                                                                                          |
| `secretName`                   | Nome do Kubernetes Secret montado no container. Guarda credenciais como variáveis de ambiente.                                                                         |
| Infisical                      | Serviço de segredos. As credenciais chegam ao código por variáveis de ambiente injetadas em runtime.                                                                   |
| tag                            | Metadado `chave:valor` do deployment (`environment`, `severity`, `code_owner`) ([Seção 13](#13-tags-de-pipeline)).                                                     |
| imagem / `Dockerfile`          | Cada pipeline roda em uma imagem Docker construída a partir da imagem base do repositório.                                                                             |
| workspace e `--package`        | O repositório é um workspace do `uv`. `uv run --package <pipeline>` executa uma pipeline com as dependências dela.                                                     |
| staging e prod                 | Os dois ambientes. Staging não tem agendamento. Prod tem.                                                                                                              |
| promoção                       | Mover uma função de `utils.py` para `src/utils/` quando uma segunda pipeline precisa dela ([Seção 11](#11-srcprefect_rj_iplanrio--código-compartilhado-do-workspace)). |
| pipeline de dump               | Pipeline que copia dados de um banco para o BigQuery. O template do repositório cria esse tipo.                                                                        |
| `{{ build-image.tag }}`        | Valor que o Prefect preenche no deploy com a tag da imagem construída. Vem do passo `build-image` do `prefect.yaml`.                                                   |
| `{{ get-commit-hash.stdout }}` | Valor que o Prefect preenche no deploy com o hash curto do commit. Vem do passo `get-commit-hash`.                                                                     |

## Apêndice A — Por que tags em vez de parâmetros

| Aspecto             | Tags                                             | Parâmetros                                  |
| ------------------- | ------------------------------------------------ | ------------------------------------------- |
| **Definição**       | Metadados imutáveis da infraestrutura            | Configuração mutável do flow                |
| **Escopo**          | Visíveis no Prefect Cloud; usado em filtros      | Passados como argumentos ao flow            |
| **Mutabilidade**    | Alteradas apenas no `prefect.yaml`               | Podem ser sobrescritas em tempo de execução |
| **Observabilidade** | Automaticamente propagadas aos logs estruturados | Requerem injeção manual no flow             |
| **Contaminação**    | O flow fica limpo; sem parâmetros "framework"    | O flow acumula parâmetros de infraestrutura |

Tags separam **infraestrutura** (Prefect) de **negócio** (parâmetros do flow).
