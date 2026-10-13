---
lang: pt-br
---

# Guia de Estilo

Regras para escrever, estruturar e manter o pacote `iplanrio`. Cada regra declara o que é, por que existe e apresenta um exemplo concreto extraído do código.

## Índice

1. [Filosofia](#1-filosofia)
2. [Critérios de promoção](#2-critérios-de-promoção)
3. [Estrutura de módulos](#3-estrutura-de-módulos)
4. [API pública e contratos de import](#4-api-pública-e-contratos-de-import)
5. [Estilo Python](#5-estilo-python)
6. [Validação de dados](#6-validação-de-dados)
7. [Padrões do Prefect](#7-padrões-do-prefect)
8. [Testes](#8-testes)
9. [Higiene do repositório](#9-higiene-do-repositório)

## 1. Filosofia

`iplanrio` é a **camada compartilhada de primitivos** na arquitetura de três camadas documentada no `STYLEGUIDE.md` raiz. É um membro do workspace uv (`packages/iplanrio/`) consumido por todas as pipelines deste monorepo.

| Camada                    | Local                         | Regra                                                                   |
| ------------------------- | ----------------------------- | ----------------------------------------------------------------------- |
| **Compartilhado de primitivos** | `packages/iplanrio/` (este pacote) | Primitivos reutilizáveis sem lógica de pipeline. Consuma, não duplique. |
| **Compartilhado do repo** | `src/prefect_rj_iplanrio/`    | Utilitários usados por 2+ pipelines no mesmo monorepo.                  |
| **Interno à pipeline**    | `pipelines/rj_x__y/utils.py`  | Lógica que pertence a exatamente uma pipeline.                          |

**Regra central:** este pacote fornece primitivos. Lógica de domínio — regras de negócio, transformações específicas de uma secretaria, queries SQL de uma pipeline — pertence à pipeline que a originou. Quando uma função aqui começa a acumular `if dataset_id == "brutos_x":`, ela desceu um nível demais.

## 2. Critérios de promoção

### 2.1 Quando promover

Uma função sobe para `iplanrio` quando **todas** as condições forem verdadeiras simultaneamente:

1. Duas ou mais pipelines precisam dela e ela é um primitivo genérico, sem relação com o domínio de nenhuma delas (caso contrário, pertence ao `src/` do repo).
2. Não contém lógica específica de nenhuma pipeline, secretaria ou dataset.
3. É estável o suficiente para ser tratada como contrato de API pública — renomeá-la depois quebra consumidores silenciosamente.

Promoção especulativa é proibida. "Isso pode ser útil em outro lugar" não é motivo. Copie uma vez; promova no segundo uso confirmado.

### 2.2 O que nunca pertence aqui

| Artefato                            | Motivo                                                                      |
| ----------------------------------- | --------------------------------------------------------------------------- |
| Lógica de negócio de uma secretaria | Pertence ao `utils.py` da pipeline correspondente.                          |
| Queries SQL                         | Vivem em `queries/*.sql` dentro da pipeline que as usa.                     |
| Constantes de dataset ou table ID   | São dados de configuração de cada pipeline, não primitivos de biblioteca.   |
| `@flow`                             | Flows são deployments — pertencem aos repositórios consumidores.            |
| Código gerado por LLM sem revisão   | Arquivos de migração, resumos de sessão e notas temporárias não são código. |

## 3. Estrutura de módulos

### 3.1 Organização de módulos

O pacote divide-se em dois subpacotes:

**`utils/`** — primitivos independentes de domínio, sem lógica de pipeline. Cada módulo cobre exatamente um domínio técnico (armazenamento, credenciais, I/O de dados, transformação de DataFrame, conectores de banco). Nomeie pelo domínio, não pelo papel (`polars.py`, não `transformations.py`).

Módulos em `utils/` são de dois tipos:

- **Puro:** sem import de Prefect — funções de transformação, I/O de arquivo, parsing. Totalmente testável sem subir um ambiente Prefect.
- **Com fronteira Prefect:** contém pares `função_pura` + `@task` wrapper. O `@task` é um invólucro fino; a lógica vive na função pura do mesmo módulo.

**`pipelines_templates/`** — implementações de padrões comuns de extração, organizadas em subpacotes por template. Cada subpacote contém:

- `tasks.py`: apenas `@task`, delega para `utils.py`.
- `utils.py`: lógica pura de extração e particionamento.

`@flow` é proibido em qualquer lugar do pacote (ver [§7.2](#72-flow--proibido)).

### 3.2 Regra de separação dentro de um módulo

Um módulo que declara `@task` separa as tarefas das funções puras:

```python
# ✅ correto — função pura testável + wrapper @task fino

def create_table_and_upload_to_gcs(
    data_path: Path,
    dataset_id: str,
    table_id: str,
    dump_mode: str,
) -> Path:
    """Cria tabela no BigQuery e faz upload dos dados para o GCS."""
    ...


@task
def create_table_and_upload_to_gcs_task(
    data_path: Path,
    dataset_id: str,
    table_id: str,
    dump_mode: str,
) -> Path:
    """Wrapper Prefect de ``create_table_and_upload_to_gcs``."""
    return create_table_and_upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
    )
```

```python
# ❌ errado — lógica de negócio dentro do @task
@task
def create_table_and_upload_to_gcs_task(data_path: Path, dataset_id: str, ...) -> Path:
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    st = bd.Storage(...)
    # ... 80 linhas de lógica
```

A função pura é o que os testes chamam. O `@task` é o que o Prefect vê.

## 4. API pública e contratos de import

### 4.1 Caminhos de import são contratos

Como `iplanrio` é consumido por todas as pipelines sem versionamento semântico formal, cada caminho de import é um contrato implícito. Renomear ou mover um símbolo público quebra todos os consumidores silenciosamente — sem erro de compilação, sem aviso de deprecação automático.

```python
# Consumidor em prefect_rj_iplanrio depende deste import exato
from iplanrio.utils.bd import create_table_and_upload_to_gcs_task
```

Renomear `bd.py` para `bigquery.py` ou mover `create_table_and_upload_to_gcs_task` para outro módulo quebra esse consumidor na próxima vez que o container for construído.

### 4.2 Regras de compatibilidade

| Operação                        | Seguro?    | Ação necessária                                              |
| ------------------------------- | ---------- | ------------------------------------------------------------ |
| Adicionar nova função           | ✅ Sim     | Nenhuma                                                      |
| Adicionar parâmetro com default | ✅ Sim     | Documente o novo parâmetro                                   |
| Remover parâmetro com default   | ⚠️ Cuidado | Verifique todos os consumidores antes                        |
| Renomear função                 | ❌ Não     | Coordene com todos os consumidores; atualize simultaneamente |
| Mover função entre módulos      | ❌ Não     | Idem — ou mantenha alias de import no módulo original        |
| Alterar tipo de retorno         | ❌ Não     | Quebrará consumidores que dependem do tipo anterior          |

### 4.3 Visibilidade sem prefixo `_`

Não use `_` como sinal de "função interna". Se uma função não deve ser importada por consumidores externos, mantenha-a no módulo onde pertence. Se está visível em um módulo público, trate-a como API pública.

```python
# ❌ errado — prefixo _ não impede import externo e gera falsa sensação de segurança
def _delete_prod_dataset(only_staging_dataset: bool, dataset_id: str) -> None:
    ...

# ✅ correto — se a função é interna, mantenha-a apenas onde é usada;
# se é necessária em outro módulo, exponha-a sem prefixo e documente
def delete_prod_dataset(only_staging_dataset: bool, dataset_id: str) -> None:
    ...
```

### 4.4 Alias de compatibilidade durante migração

Ao renomear um módulo público, mantenha o caminho antigo re-exportando tudo do novo enquanto os consumidores são atualizados. O alias é dívida — não adicione novos símbolos a ele.

```python
# old_name/__init__.py  ← arquivo temporário de migração
# Não adicione novos símbolos aqui. Atualize os consumidores e remova este arquivo.
from iplanrio.new_name import *  # noqa: F401, F403
```

O alias é a única situação em que `import *` e `# noqa: F401, F403` são tolerados — exclusivamente no arquivo de compatibilidade, jamais em código de produção.

## 5. Estilo Python

### 5.1 Type hints

Todas as assinaturas de função — parâmetros e tipo de retorno — devem ter type hints. Sem exceções.

```python
# ✅ correto
def fetch_batch(self, batch_size: int) -> list[list]:
    ...

# ❌ errado — tipo de retorno ausente, tipos dos parâmetros ausentes
def fetch_batch(self, batch_size):
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

O módulo `typing` ainda é usado para `TypedDict`, `Protocol` e `TYPE_CHECKING` — esses não têm equivalentes embutidos.

### 5.3 Imports

- Sem imports com wildcard (`from x import *`), exceto no alias de compatibilidade descrito em [§4.4](#44-alias-de-compatibilidade-durante-migração).
- Sem cabeçalho de encoding (`# -*- coding: utf-8 -*-`). UTF-8 é o padrão no Python 3; o cabeçalho é ruído.
- Imports explícitos apenas: liste cada nome que você usa.
- Sem `# pylint: disable=...` ou `# noqa` sem identificador de regra. Corrija o problema em vez de silenciá-lo. Se o silêncio for inevitável, use `# noqa: EXXX` com um comentário explicando por quê.

```python
# ✅ correto
from iplanrio.utils.bd import create_table_and_upload_to_gcs
from iplanrio.utils.env import get_bd_credentials_from_env

# ❌ errado
from iplanrio.utils.bd import *
```

### 5.4 Nomenclatura

- Funções e variáveis: `snake_case`.
- Classes: `PascalCase`.
- Constantes de módulo: `UPPER_SNAKE_CASE` apenas no nível de módulo, nunca dentro de funções.
- Sem prefixo `_` para indicar privacidade (ver [§4.3](#43-visibilidade-sem-prefixo-_)).

```python
# ❌ errado — UPPERCASE dentro de função não é constante, é variável local com nome enganoso
def dump_url_flow(...):
    DATA_PATH = "/tmp/dump_url/"
    DOWNLOAD_URL_TASK = download_url(url=url, fname=DATA_FNAME)
```

```python
# ✅ correto — constante real no nível de módulo
DEFAULT_TMP_PATH = "/tmp/dump_url/"

def dump_url(...):
    data_path = DEFAULT_TMP_PATH
    result = download_url(url=url, fname=data_path)
```

### 5.5 Logging

**Módulos puros** (sem import de Prefect) usam `logging.getLogger(__name__)` da stdlib diretamente:

```python
# ✅ correto — módulo puro, sem Prefect
import logging

logger = logging.getLogger(__name__)


def fetch_stations(dataset_id: str, table_id: str) -> str:
    """Busca estações para o dataset e tabela informados."""
    logger.info("Buscando estações: dataset_id=%s, table_id=%s", dataset_id, table_id)
    ...
```

**Módulos com `@task`** usam `prefect.get_run_logger()` diretamente dentro da função decorada:

```python
# ✅ correto — dentro de @task, usa o logger do Prefect diretamente
from prefect import get_run_logger, task


@task
def rename_current_flow_run_task(new_name: str) -> None:
    """Renomeia a execução de fluxo atual."""
    logger = get_run_logger()
    logger.info("Renomeando execução para: %s", new_name)
    ...
```

`print()` é **proibido** em qualquer arquivo do pacote.

### 5.6 Docstrings

Todas as funções públicas devem ter uma docstring no formato reST. A linha de resumo usa modo imperativo e termina com ponto final. Como type hints já cobrem os tipos, as diretivas `:type:` e `:rtype:` são redundantes e devem ser omitidas.

```python
# ✅ correto
def query_to_line(query: str) -> str:
    """Converte uma query multilinha em uma única linha normalizada.

    Remove quebras de linha, tabs e espaços redundantes. Útil para
    embutir queries em logs e mensagens de erro sem ruído visual.

    :param query: String SQL potencialmente multilinha.
    :returns: Query em uma única linha com espaços normalizados.
    """
    ...
```

```python
# ❌ errado — sem docstring
def query_to_line(query: str) -> str:
    query = textwrap.dedent(query)
    return " ".join([line.strip() for line in query.split("\n")])
```

Regras:

- `:returns:` é omitido quando o tipo de retorno é `None`.
- `:raises:` lista todas as exceções que a função pode lançar intencionalmente.
- Um `@task` que só delega para a função pura correspondente pode ter apenas a linha de resumo.

### 5.7 Parâmetros em excesso

Funções com mais de 5 parâmetros que formem um grupo coeso devem agrupar esses parâmetros em um `dataclass` ou `TypedDict`. A regra não se aplica a `@task`, cujos parâmetros precisam ser JSON-serializáveis para o Prefect.

Prefira `dataclass(frozen=True)` para configs imutáveis com defaults. Use `TypedDict` quando o caller precisa construir o dict e passá-lo com `**`.

```python
# ❌ errado — mais de 5 parâmetros sem agrupamento
def database_get_db(
    database_type: str,
    hostname: str,
    port: int,
    user: str,
    password: str,
    database: str,
    charset: str = NOT_SET,
) -> Database:
    ...
```

```python
# ✅ correto — dataclass para configuração de conexão
from dataclasses import dataclass


@dataclass(frozen=True)
class DatabaseConfig:
    """Parâmetros de conexão para um banco de dados relacional."""

    database_type: str
    hostname: str
    port: int
    user: str
    password: str
    database: str
    charset: str = NOT_SET


def database_get_db(config: DatabaseConfig) -> pl.DataFrame:
    """Instancia o objeto de banco de dados correto para o tipo informado."""
    ...
```

### 5.8 Exceções

Não levante `BaseException` diretamente. Use o tipo de exceção mais específico disponível ou crie uma subclasse própria.

```python
# ❌ errado
raise BaseException("Data need to be a pandas DataFrame")

# ✅ correto
raise TypeError(f"Esperado pl.DataFrame, recebido {type(data).__name__}.")
```

### 5.9 Polars e Parquet

`pandas` é **proibido** em código novo. Use `polars` para todas as transformações de DataFrame. Para operações espaciais, use `geopolars`.

**Eager (`pl.DataFrame`)** — quando os dados cabem em memória e o processamento é batch (ex.: uma janela de extração de banco de dados):

```python
# ✅ correto — eager para batch em memória
import polars as pl


def batch_to_frame(batch: list[list], columns: list[str]) -> pl.DataFrame:
    """Converte um batch de linhas em um DataFrame Polars.

    :param batch: Lista de linhas, cada uma como lista de valores.
    :param columns: Nomes das colunas na mesma ordem que os valores.
    :returns: DataFrame com os dados do batch.
    """
    return pl.DataFrame(dict(zip(columns, zip(*batch), strict=True)))
```

**Lazy (`pl.LazyFrame`)** — quando a fonte é um arquivo Parquet e a transformação pode ser otimizada pelo motor de query:

```python
# ✅ correto — lazy para pipeline arquivo → arquivo
from pathlib import Path

import polars as pl


def normalize_parquet(source: Path, dest: Path) -> None:
    """Normaliza nomes de colunas de um arquivo Parquet e escreve o resultado.

    :param source: Caminho do arquivo Parquet de entrada.
    :param dest: Caminho do arquivo Parquet de saída.
    """
    lf = pl.scan_parquet(source)
    renamed = {col: col.lower().replace(" ", "_") for col in lf.columns}
    lf.rename(renamed).sink_parquet(dest)
```

**Parquet** é o único formato de serialização intermediária. CSV é proibido em código novo, exceto quando a API de destino exige explicitamente (ex.: integração legada com `basedosdados` que não suporte Parquet).

```python
# ❌ errado — CSV como formato intermediário
df.write_csv("/tmp/data.csv")

# ✅ correto — Parquet como formato intermediário
df.write_parquet("/tmp/data.parquet")
```

## 6. Validação de dados

Duas camadas, cada uma com um papel distinto:

| Camada                     | Ferramenta                  | Quando usar                           |
| -------------------------- | --------------------------- | ------------------------------------- |
| Estrutura de DataFrame     | Polars nativo (`pl.Schema`) | Na borda de entrada de dados externos |
| Parâmetros e configurações | Pydantic v2 `BaseModel`     | Params de `@flow`, configs de conexão |

### 6.1 Polars schema — validação de estrutura na borda

Declare o schema esperado explicitamente ao ler dados de fontes externas (banco, GCS, API). Polars levanta `SchemaError` imediatamente se o dado não corresponder — falha explícita e próxima da origem.

```python
# ✅ correto — schema declarado na borda, falha explícita
import polars as pl
from pathlib import Path

STATIONS_SCHEMA = pl.Schema({
    "station_id": pl.String,
    "latitude": pl.Float64,
    "longitude": pl.Float64,
    "last_update": pl.Datetime,
})


def read_stations(path: Path) -> pl.DataFrame:
    """Lê estações do Parquet e valida o schema na borda.

    :param path: Caminho do arquivo Parquet.
    :returns: DataFrame com o schema de estações validado.
    :raises polars.exceptions.SchemaError: Se o arquivo não respeitar o schema esperado.
    """
    return pl.read_parquet(path, schema=STATIONS_SCHEMA)
```

```python
# ❌ errado — sem schema, erros de tipo propagam silenciosamente para o BigQuery
def read_stations(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path)
```

**Onde declarar schemas:** constantes de schema vivem em `constants.py` quando compartilhadas entre dois ou mais módulos; caso contrário, no próprio módulo que as usa.

### 6.2 Pydantic v2 — validação de parâmetros e configurações

Use `pydantic.BaseModel` com `@field_validator` (v2) para validar parâmetros de `@flow` e objetos de configuração. `@validator` (v1) é proibido em código novo.

```python
# ✅ correto — Pydantic v2 com @field_validator
from pydantic import BaseModel, field_validator


class UploadConfig(BaseModel):
    """Configuração de upload para o BigQuery."""

    dataset_id: str
    table_id: str
    dump_mode: str
    environment: str = "prod"

    @field_validator("dump_mode")
    @classmethod
    def validate_dump_mode(cls, v: str) -> str:
        """Valida que dump_mode seja 'append' ou 'overwrite'."""
        if v not in {"append", "overwrite"}:
            raise ValueError(f"dump_mode deve ser 'append' ou 'overwrite', recebido: {v!r}")
        return v

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Valida que environment seja 'prod' ou 'staging'."""
        if v not in {"prod", "staging"}:
            raise ValueError(f"environment deve ser 'prod' ou 'staging', recebido: {v!r}")
        return v
```

```python
# ❌ errado — @validator é Pydantic v1, depreciado
from pydantic import validator

class UploadConfig(BaseModel):
    dump_mode: str

    @validator("dump_mode")
    def validate_dump_mode(cls, v):
        ...
```

### 6.3 O que nunca fazer

- **Validação com efeitos colaterais.** `@field_validator` verifica estrutura e formato — I/O (query ao BigQuery, leitura de GCS, chamada de API) pertence ao corpo da função que usa o dado, nunca dentro do validador.

```python
# ❌ errado — I/O dentro de validador torna testes impossíveis
@field_validator("campaign_name")
@classmethod
def validate_campaign_name(cls, v: str) -> str:
    client = bigquery.Client(...)
    result = client.query(f"SELECT COUNT(*) FROM ... WHERE name = '{v}'")
    if list(result)[0][0] == 0:
        raise ValueError("campaign_name não encontrado")
    return v

# ✅ correto — validação estrutural no modelo, I/O na função que usa o resultado
@field_validator("campaign_name")
@classmethod
def validate_campaign_name(cls, v: str) -> str:
    if not v.strip():
        raise ValueError("campaign_name não pode ser vazio")
    return v.strip()

# Na função de negócio, separado:
def verify_campaign_exists(config: UploadConfig, client: bigquery.Client) -> None:
    """Verifica que campaign_name existe no BigQuery antes do dispatch."""
    ...
```

- `assert` fora de testes como substituto de validação.
- Revalidar dados que já passaram pela borda — confie no schema Polars após a leitura inicial.

## 7. Padrões do Prefect

### 7.1 `@task`

Permitido apenas em módulos cuja responsabilidade explícita é a fronteira com o Prefect: módulos `utils/` que declaram pares `função_pura + @task`, e arquivos `tasks.py` dentro de `pipelines_templates/`. Cada `@task` é um wrapper fino que delega para uma função pura do mesmo pacote.

```python
# ✅ correto — wrapper fino, type hints completos
@task
def inject_bd_credentials_task(environment: str = "prod") -> None:
    """Injeta credenciais do BigQuery no ambiente de execução."""
    inject_bd_credentials(environment=environment)
```

```python
# ❌ errado — lógica diretamente no @task
@task
def inject_bd_credentials_task(environment: str = "prod") -> None:
    service_account_b64 = getenv_or_action(f"BASEDOSDADOS_CREDENTIALS_{environment.upper()}")
    service_account = base64.b64decode(service_account_b64)
    with open("/tmp/credentials.json", "wb") as f:
        f.write(service_account)
    environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/credentials.json"
```

### 7.2 `@flow` — proibido

`@flow` é **proibido** em qualquer arquivo deste pacote. Flows são deployments: pertencem aos repositórios consumidores. `pipelines_templates/` contém apenas `@task` — nunca um `@flow`.

**Justificativa:** um flow aqui só pode ser executado com os parâmetros padrão ou com parâmetros hardcoded. Isso transforma uma biblioteca reutilizável em uma pipeline específica mal disfarçada. Os repositórios consumidores declaram flows usando as tasks deste pacote como blocos de construção.

```python
# ❌ proibido em qualquer arquivo de iplanrio
from prefect import flow

@flow(log_prints=True)
def rj_iplanrio__equipamentos_arcgis(url: str, dataset_id: str, table_id: str) -> None:
    ...
```

### 7.3 Ordenação de tasks — data flow em vez de `wait_for`

Expresse a ordem via dependências de dados. `wait_for` é reservado para casos em que não há relação de dados entre as tasks.

```python
# ✅ correto — a ordem é inferida pelo grafo de dados
credentials = inject_bd_credentials_task(environment="prod")
path = download_data_task(url=url, wait_for=[credentials])  # download_data_task não usa o output de credentials

# ❌ errado — wait_for onde existe dependência de dados
fetch_task(wait_for=[process_task(raw)])  # ignora o valor de retorno; use fetch_task(data=process_task(raw))
```

### 7.4 Padrões proibidos

| Padrão                                            | Por que é proibido                                                                 |
| ------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `@flow` em qualquer arquivo                       | Flows são deployments dos repositórios consumidores, não primitivos de biblioteca. |
| `@task` fora de módulos com essa responsabilidade | Mistura responsabilidades; dificulta localizar onde estão as tarefas.              |
| `if __name__ == "__main__":`                      | Scripts ad-hoc não pertencem a uma biblioteca.                                     |
| Variáveis UPPERCASE dentro de funções             | Reservado para constantes de módulo; dentro de funções é ruído visual.             |

## 8. Testes

### 8.1 Estrutura

A estrutura de `tests/` espelha a do pacote: cada módulo de produção tem um arquivo de teste correspondente com o mesmo caminho relativo, prefixado por `test_`. Um arquivo de teste nunca testa dois módulos simultaneamente.

```
# módulo de produção                          → arquivo de teste
iplanrio/utils/polars.py                      → tests/utils/test_polars.py
iplanrio/utils/io.py                          → tests/utils/test_io.py
pipelines_templates/<t>/utils.py              → tests/pipelines_templates/<t>/test_utils.py
```

### 8.2 Table-driven testing com `pytest.mark.parametrize`

Funções determinísticas com múltiplos casos de entrada usam `@pytest.mark.parametrize`. O ID de cada caso descreve o cenário, não o índice. O tuple de parâmetros é sempre tipado explicitamente na assinatura do teste.

```python
# ✅ correto
import pytest
from iplanrio.utils.io import query_to_line


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("SELECT\n  *\nFROM t", "SELECT * FROM t"),
        ("  SELECT  *  FROM  t  ", "SELECT * FROM t"),
        ("SELECT * FROM t", "SELECT * FROM t"),
    ],
    ids=["multiline", "extra_spaces", "already_normalized"],
)
def test_query_to_line(query: str, expected: str) -> None:
    assert query_to_line(query) == expected
```

```python
# ❌ errado — IDs ausentes, dificulta diagnóstico de falhas
@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("SELECT\n  *\nFROM t", "SELECT * FROM t"),
        ("  SELECT  *  FROM  t  ", "SELECT * FROM t"),
    ],
)
def test_query_to_line(query, expected):
    assert query_to_line(query) == expected
```

### 8.3 Property-based testing com `hypothesis`

Propriedades invariantes de funções de transformação pura são verificadas com `hypothesis`. Use `@given` com estratégias de `hypothesis.strategies`. Declare sempre o tipo do argumento gerado na assinatura do teste.

Casos de uso típicos neste pacote:

| Propriedade                               | Função                                        | Estratégia                                   |
| ----------------------------------------- | --------------------------------------------- | -------------------------------------------- |
| Idempotência de normalização de colunas   | `normalize_columns`                           | `st.lists(st.text(min_size=1), unique=True)` |
| Ausência de quebras de linha              | `query_to_line`                               | `st.text()`                                  |
| Preservação de linhas não vazias          | `parse_comma_separated_string_to_list`        | `st.lists(st.text(min_size=1))`              |
| Roundtrip de datas válidas                | `is_date`                                     | `st.dates()`                                 |
| Preservação de contagem de linhas         | `batch_to_frame`                              | `st.integers(min_value=1, max_value=1000)`   |
| Estabilidade de schema após transformação | qualquer função `pl.DataFrame → pl.DataFrame` | `st.fixed_dictionaries(...)`                 |

```python
# ✅ correto — idempotência de normalização de colunas com Polars
import polars as pl
from hypothesis import given
from hypothesis import strategies as st
from iplanrio.utils.polars import normalize_columns


@given(st.lists(st.text(min_size=1), min_size=1, unique=True))
def test_normalize_columns_is_idempotent(columns: list[str]) -> None:
    """Normalizar nomes de colunas duas vezes produz o mesmo resultado."""
    df = pl.DataFrame(schema={col: pl.String for col in columns})
    first = normalize_columns(df).columns
    df2 = pl.DataFrame(schema={col: pl.String for col in first})
    second = normalize_columns(df2).columns
    assert first == second
```

```python
# ✅ correto — preservação de contagem de linhas e schema em batch_to_frame
import polars as pl
from hypothesis import given
from hypothesis import strategies as st
from iplanrio.utils.polars import batch_to_frame


@given(st.integers(min_value=1, max_value=1000))
def test_batch_to_frame_preserves_row_count(n_rows: int) -> None:
    """batch_to_frame produz exatamente n_rows linhas."""
    columns = ["id", "value"]
    batch = [[i, i * 2] for i in range(n_rows)]
    df = batch_to_frame(batch, columns)
    assert df.height == n_rows
    assert set(df.columns) == set(columns)
```

```python
# ✅ correto — ausência de quebras de linha após normalização de query
from hypothesis import given
from hypothesis import strategies as st
from iplanrio.utils.io import query_to_line


@given(st.text())
def test_query_to_line_has_no_newlines(query: str) -> None:
    """O resultado de query_to_line nunca contém quebras de linha."""
    result = query_to_line(query)
    assert "\n" not in result
    assert "\r" not in result
```

### 8.4 Dependências externas

Funções que chamam GCS, BigQuery ou bancos de dados não têm testes de integração automáticos neste pacote. A estratégia é isolar a lógica pura:

- Lógica de transformação e decisão fica em funções puras testadas diretamente.
- I/O e chamadas de rede ficam nas bordas — nas funções que chamam `bd.Table`, `storage.Client`, etc.
- Mocks são permitidos apenas para verificar **contratos de interface** (ex.: que `execute_query` é chamado com a query correta), nunca para testar lógica de negócio encapsulada no mock.

```python
# ✅ correto — testa a lógica pura, não o I/O
def test_build_single_partition_query_oracle_format() -> None:
    result = build_single_partition_query(
        query="SELECT * FROM tabela",
        partition_column="data_ref",
        lower_bound_date=None,
        last_partition_date="2024-01-01",
        date_format="%Y-%m-%d",
        database_type="oracle",
        offset=1,
    )
    assert "TO_DATE" in result["query"]
    assert result["start_date"] == "2024-01-01"
```

```python
# ❌ errado — mock que testa o mock, não a lógica
def test_create_table_calls_bd(monkeypatch):
    mock_table = MagicMock()
    monkeypatch.setattr("iplanrio.utils.bd.bd.Table", mock_table)
    create_table_and_upload_to_gcs(data_path=..., ...)
    mock_table.assert_called_once()  # só verifica que o mock foi chamado
```

### 8.5 O que nunca fazer

- `assert` fora de arquivos de teste.
- Testes que dependem de credenciais reais, rede ou estado externo.
- Fixtures com efeitos colaterais persistentes (ex.: criar arquivos em `/tmp` sem limpeza).
- Testes sem asserção — um teste que passa incondicionalmente não testa nada.

## 9. Higiene do repositório

### 9.1 Proibido commitar

| Artefato                                                | Motivo                                                                                                 |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `# -*- coding: utf-8 -*-`                               | UTF-8 é o padrão no Python 3. O cabeçalho é ruído e será removido pelo pre-commit.                     |
| Código comentado em bloco                               | Use `git log` para recuperar código removido. Comentários de centenas de linhas são dívida de leitura. |
| Arquivos `.py` completamente vazios                     | Delete-os ou preencha-os com conteúdo justificado.                                                     |
| Variáveis UPPERCASE dentro de funções                   | Reservado para constantes de módulo no nível de módulo.                                                |
| `# pylint: disable=...` sem explicação                  | Corrija o problema. Se o silêncio for inevitável, use `# noqa: EXXX  # motivo`.                        |
| `# noqa` sem identificador de regra                     | `# noqa` desativa todos os avisos da linha — um erro de linter não diagnosticado pode esconder outro.  |
| `FIX_SUMMARY.md`, `MIGRATION_NOTES.md`, notas de sessão | Artefatos de sessões de LLM. Delete antes de commitar.                                                 |

### 9.2 `.gitignore` mínimo

```gitignore
# Python
__pycache__/
*.py[cod]
.ruff_cache/

# Ambientes
.env
.venv

# Build
*.egg-info/
dist/

# macOS
.DS_Store
```

### 9.3 `pyproject.toml` — `description`

Uma frase que diga a um leitor — que nunca viu este pacote — o que ele faz e por que existe.

```toml
# ✅ correto
description = "Shared utilities and Prefect task wrappers for iplanrio data pipelines, providing BigQuery upload, credential injection, database connectors and Polars DataFrame transformations."

# ❌ errado — texto padrão sem informação
description = "Add your description here"
```
