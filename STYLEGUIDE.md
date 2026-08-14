---
lang: pt-br
---

# Guia de Estilo

Regras para escrever, estruturar e manter pipelines neste monorepo. Cada regra declara o que é, por que existe e apresenta um exemplo concreto extraído do código.

## Índice

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
13. [Labels de pipeline](#13-labels-de-pipeline)

## 1. Filosofia

Flows orquestram. Tasks encapsulam. Utils calculam. SQL é dado.

Qualquer pessoa deve conseguir abrir um diretório de pipeline e entender imediatamente sua estrutura, pois toda pipeline segue as mesmas convenções. Consistência vale mais do que otimização local.

## 2. Arquitetura de código em três camadas

O código neste repositório vive em um de três níveis. O nível determina onde uma função é escrita, não sua importância.

| Camada                    | Local                        | Regra                                                                                        |
| ------------------------- | ---------------------------- | -------------------------------------------------------------------------------------------- |
| **Compartilhado externo** | `iplanrio` (git dependency)  | Utilitários cross-repo. Consuma, não modifique aqui.                                         |
| **Compartilhado do repo** | `src/prefect_rj_iplanrio/`   | Utilitários usados por **2 ou mais pipelines** neste repo sem lógica específica de pipeline. |
| **Interno à pipeline**    | `pipelines/rj_x__y/utils.py` | Lógica que pertence a exatamente uma pipeline.                                               |

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
└── queries/         ← quando a pipeline executa SQL
    ├── fetch_data.sql
    └── get_last_update.sql
```

### 3.3 Proibidos nos diretórios de pipeline

O seguinte nunca deve ser commitado num diretório de pipeline:

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
"""Flow for rj_cor__meteorologia_inmet."""

from prefect import flow
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task

from pipelines.rj_cor__meteorologia_inmet.tasks import fetch_stations_task, upload_task


@flow(log_prints=True)
def rj_cor__meteorologia_inmet(
    dataset_id: str = "brutos_meteorologia_inmet",
    table_id: str = "estacoes",
) -> None:
    rename_current_flow_run_task(new_name=table_id)
    inject_bd_credentials_task(environment="prod")
    data_path = fetch_stations_task(dataset_id=dataset_id, table_id=table_id)
    upload_task(data_path=data_path, dataset_id=dataset_id, table_id=table_id)
```

**Nunca contém:**

- O decorator `@task`
- `if __name__ == "__main__":`
- Lógica de negócio (busca de dados, transformação, chamadas de API)
- Strings SQL

### 4.2 `tasks.py`

**Contém:** exclusivamente funções decoradas com `@task`. Cada task é um wrapper fino que chama uma função de `utils.py` ou da biblioteca `iplanrio`.

**Papel:** fronteira entre o engine de orquestração do Prefect e o Python puro. Tasks dão visibilidade ao Prefect — retries, rastreamento de estado, limites de concorrência — sem acoplar essa maquinaria à lógica em si.

```python
# ✅ correto — task delega para utils, permanece fina
from prefect import task
from pipelines.rj_cor__meteorologia_inmet import utils


@task
def fetch_stations_task(dataset_id: str, table_id: str) -> str:
    return utils.fetch_stations(dataset_id=dataset_id, table_id=table_id)
```

```python
# ❌ errado — 80 linhas de lógica de API dentro de um @task
@task
def fetch_stations_task(dataset_id: str, table_id: str) -> str:
    session = requests.Session()
    session.headers.update(...)
    response = session.get(...)
    # ... mais 70 linhas
```

**Nunca contém:** `@flow`, lógica de negócio com mais de ~20 linhas, `import`s sem relação com o Prefect ou com a chamada a `utils`.

### 4.3 `utils.py`

**Contém:** funções Python sem dependência do Prefect que realizam o trabalho real — chamadas de API, transformação de dados, I/O de arquivos e construção de queries.

**Papel:** o núcleo testável da pipeline. Por não terem imports do Prefect, essas funções podem ser chamadas diretamente no `pytest` sem subir um ambiente Prefect.

```python
# ✅ correto — sem Prefect, totalmente testável
import requests
import pandas as pd


def fetch_stations(dataset_id: str, table_id: str) -> str:
    response = requests.get("https://api.inmet.gov.br/estacoes/T")
    response.raise_for_status()
    df = pd.DataFrame(response.json())
    path = f"/tmp/{dataset_id}/{table_id}.csv"
    df.to_csv(path, index=False)
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
def fetch_stations(dataset_id: str, table_id: str) -> str:
    ...

# ❌ errado — tipo de retorno ausente, tipos dos parâmetros ausentes
def fetch_stations(dataset_id, table_id):
    ...
```

### 5.2 Sintaxe de tipos

Use a sintaxe de union do Python 3.10+ e os tipos genéricos embutidos. Não importe tipos de container do módulo `typing`.

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

```python
# ✅ correto
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task

# ❌ errado
from iplanrio.pipelines_utils.bd import *
```

### 5.4 Nomenclatura

Não use `_` como prefixo para indicar nomes internos/privados. Use fronteiras de módulo para controlar visibilidade — se algo não deve ser importado por outros módulos, mantenha-o no módulo onde pertence, não em um módulo globalmente visível com uma dica no nome.

### 5.5 Logging

`print()` é **proibido** em qualquer arquivo Python do repositório. Todo logging usa o objeto pré-configurado de `prefect_rj_iplanrio.logging`, que integra OpenTelemetry e garante formato e destino uniformes em todas as pipelines.

```python
# ✅ correto
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


def fetch_stations(dataset_id: str, table_id: str) -> str:
    logger.info("Buscando estações para dataset_id=%s", dataset_id)
    ...
    logger.warning("Nenhuma estação encontrada")
```

```python
# ❌ errado — import direto do stdlib sem a configuração do workspace
import logging
logger = logging.getLogger(__name__)

# ❌ errado — proibido
def fetch_stations(dataset_id: str, table_id: str) -> str:
    print("Buscando estações")
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
) -> pd.DataFrame:
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


def fetch_records(config: FetchConfig) -> pd.DataFrame:
    ...
```

```python
# ✅ TypedDict — quando o caller monta o dict e passa com **params
from typing import TypedDict


class FetchParams(TypedDict):
    project: str
    dataset_id: str
    table_id: str
    environment: str
    page_size: int
    timeout: int


def fetch_records(**params: FetchParams) -> pd.DataFrame:
    ...
```

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
def fetch_data_task(url: str, timeout: int = 30) -> pd.DataFrame:
    return utils.fetch_data(url=url, timeout=timeout)
```

- Tasks ficam apenas em `tasks.py`.
- Cada task faz uma coisa.
- Type hints completos, incluindo o tipo de retorno.

### 6.3 Expressando a ordem das tasks — data flow em vez de `wait_for`

No Prefect 3, o grafo de execução é inferido a partir das dependências de dados. Quando a task B recebe o valor de retorno da task A como argumento, o Prefect sabe que A deve terminar antes de B começar. Esta é a forma correta de expressar ordenação.

`wait_for` é uma saída de emergência para o caso raro em que a task B deve rodar após a task A, mas não consome o valor de retorno de A **e** o Prefect não consegue inferir a dependência pelo grafo de chamadas.

```python
# ✅ correto — ordenação expressa por data flow
@flow(log_prints=True)
def rj_iplanrio__eai_history(dataset_id: str, table_id: str) -> None:
    rename_current_flow_run_task(new_name=table_id)
    inject_bd_credentials_task(environment="prod")           # sequencial, sem output necessário
    last_update = get_last_update_task(dataset_id=dataset_id, table_id=table_id)
    data_path = fetch_history_task(last_update=last_update)  # dependência de dados — Prefect infere a ordem
    upload_task(data_path=data_path, dataset_id=dataset_id, table_id=table_id)
```

```python
# ❌ errado — wait_for usado onde existe uma dependência de dados
inject_bd_credentials_task(environment="prod", wait_for=[rename_task])
path = download_task(url=url, wait_for=[inject_task])
```

```python
# ✅ wait_for aceitável — task de efeito colateral sem valor de retorno significativo
credentials = inject_bd_credentials_task(environment="prod")
path = download_task(url=url, wait_for=[credentials])  # apenas se download_task não usa o output de inject
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

**Justificativa:** SQL embutido em strings Python é invisível para linters e editores de SQL, não pode ser testado isoladamente e é mais difícil de revisar em pull requests. f-strings tornam a query dinâmica de um modo que não pode ser analisado estaticamente. `str.format()` colide com a sintaxe do BigQuery — `STRUCT<field STRING>` e `UNNEST([{}])` contêm o caractere `{` que `str.format()` trata como placeholder. SQL embutido como parâmetro de schedule no `prefect.yaml` produz arquivos de centenas de linhas de SQL escapado em YAML — ilegível para revisão em pull requests e invisível para qualquer linter de SQL.

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
    project="rj-iplanrio",
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
      dataset_id: brutos_wetalkie
      environment: production
```

```sql
-- ✅ queries/get_eligible_contacts.sql
SELECT *
FROM `rj-iplanrio.$dataset_id.contacts`
WHERE environment = '$environment'
```

```python
# ✅ flow.py — recebe query do Prefect e repassa para a task
from prefect import flow

from .tasks import QueryParam, fetch_data_task


@flow(log_prints=True)
def rj_secretaria__pipeline(query: QueryParam) -> None:
    fetch_data_task(query=query)
```

```python
# ✅ tasks.py
from typing import TypedDict

from prefect_rj_iplanrio.sql import load_query


class QueryParam(TypedDict):
    name: str
    replacements: dict[str, object]


def fetch_data_task(query: QueryParam) -> pd.DataFrame:
    sql = load_query(__file__, query["name"], **query["replacements"])
    ...
```

**Contrato:** as chaves de `replacements` devem corresponder exatamente aos placeholders `$variable` no arquivo `.sql`. `load_query` usa `string.Template.substitute()`, que levanta `KeyError` se um placeholder obrigatório estiver ausente — falha explícita e imediata.

Cada variante de campanha que exija SQL diferente recebe um arquivo `.sql` próprio em `queries/`. O diretório `queries/` é a única fonte de verdade do SQL da pipeline.

### 7.6 O que nunca fazer

```python
# ❌ SQL em f-string — não testável, não pesquisável
query = f"""
    SELECT max(last_update)
    FROM `rj-iplanrio.{dataset_id}_staging.{table_id}`
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

Staging e produção usam `k3s-pool`:

```yaml
work_pool:
  name: k3s-pool
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
description = "Fetches hourly precipitation data from INMET weather stations and loads it into BigQuery for COR's meteorological monitoring."

# ❌ errado — texto gerado automaticamente sem informação
description = "Pipeline meteorologia_inmet da secretaria cor"

# ❌ errado — padrão do cookiecutter não substituído
description = "TODO: replace with one sentence describing what this pipeline does and why."
```

### 10.2 `version`

Sempre `1.0.0`. O versionamento é tratado no nível do workspace via git tags e pela tag da imagem Docker construída a partir do hash do commit. Versões individuais de pacotes de pipeline não têm significado em um monorepo.

## 11. `src/prefect_rj_iplanrio/` — código compartilhado do workspace

### 11.1 Critérios de promoção

Uma função é promovida do `utils.py` de uma pipeline para `src/prefect_rj_iplanrio/` quando **ambas** as condições forem verdadeiras:

1. Duas ou mais pipelines precisam dela.
2. Não contém lógica específica de nenhuma pipeline.

Promoção especulativa ("isso pode ser útil em outro lugar") não é motivo. Copie uma vez; promova no segundo uso.

### 11.2 API pública atual

| Módulo                        | Símbolo                                   | Propósito                                                                                             |
| ----------------------------- | ----------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `prefect_rj_iplanrio.logging` | `get_logger(name)`                        | Retorna um logger pré-configurado com integração OpenTelemetry. Passe `__name__` como argumento.      |
| `prefect_rj_iplanrio.sql`     | `load_query(caller_file, name, **params)` | Carrega e renderiza um arquivo `.sql` do diretório `queries/` relativo ao chamador. Passe `__file__`. |

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

Na dúvida, pergunte: "Um novo membro do time lendo este arquivo entenderia por que ele existe e o que fazer com ele?" Se a resposta for não, apague-o.

## 13. Labels de pipeline

### 13.1 O que são labels

Toda pipeline **nova** deve ter dois labels definidos no `prefect.yaml`:
- **`code_owner`**: GitHub username do desenvolvedor responsável
- **`severity`**: Criticidade (`"low"`, `"medium"`, `"high"`, `"critical"`)

Esses labels são propagados a todos os logs estruturados e usados pelo sistema de observabilidade para atribuição de alertas e priorização de incidentes.

**Nota:** Pipelines existentes sem labels continuam funcionando normalmente (backward compatibility). A validação só é aplicada quando os labels estão presentes.

### 13.2 Como definir labels

Labels são definidos **uma única vez** na seção `parameters:` de cada deployment:

```yaml
deployments:
  - name: rj-secretaria--pipeline--prod
    parameters:
      code_owner: "seu_username"    # GitHub username
      severity: "high"              # low, medium, high, critical
      dataset_id: "brutos_data"     # ... outros parâmetros
```

### 13.3 Como usá-los no flow

No `@flow`, receba como parâmetros padrão e injete no contexto com uma linha:

```python
from prefect import flow
from prefect_rj_iplanrio.labels import set_labels, SeverityLevel

@flow(log_prints=True)
def rj_secretaria__pipeline(
    code_owner: str = "unassigned",
    severity: SeverityLevel = "medium",
    dataset_id: str = "",
) -> None:
    """Pipeline para secretaria."""
    set_labels(code_owner=code_owner, severity=severity)
    # Resto da pipeline...
```

Pronto. Todos os logs herdam as labels automaticamente.

### 13.4 Validação

O CI valida em todo PR que modifica `prefect.yaml`. Quando labels estão presentes em um deployment:
- `code_owner` não pode estar vazio
- `severity` deve ser uma das 4 opções válidas
- Se um label está presente, o outro também deve estar

Deployments sem labels são ignorados (backward compatibility). Se um deployment violar essas regras, o workflow rejeita o PR.
