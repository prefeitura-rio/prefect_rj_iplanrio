# Documentação da Migração TaxiRio - Prefect 1.4 → 3.0

## Resumo Executivo

**Data da Migração**: 2026-05-03  
**Repositório Original**: https://github.com/prefeitura-rio/pipelines_rj_iplanrio/tree/main/pipelines/taxirio  
**Versão Prefect**: 1.4 → 3.0  
**Estratégia**: Consolidação de múltiplos flows em um flow unificado parametrizado

---

## Mudanças Implementadas

### 1. Arquitetura do Flow

#### ❌ Antes (Prefect 1.4)
```
pipelines/taxirio/
├── cities/
│   ├── flows.py          # Flow individual para cities
│   ├── constants.py
│   └── mongodb.py
├── drivers/
│   ├── flows.py          # Flow individual para drivers
│   ├── constants.py
│   └── mongodb.py
├── races/
│   ├── flows.py          # Flow individual para races
│   ├── constants.py
│   └── mongodb.py
... (9 flows separados no total)
```

#### ✅ Agora (Prefect 3.0)
```
pipelines/rj_iplanrio__taxirio/
├── flow.py               # Flow ÚNICO parametrizado
├── tasks.py              # Tasks reutilizáveis
├── constants.py          # Configuração centralizada
├── cities/
│   └── mongodb.py        # Apenas schema e pipeline
├── drivers/
│   └── mongodb.py
... (schemas organizados por tabela)
```

**Benefícios:**
- ✅ Redução de 9 flows para 1 flow unificado
- ✅ Menor duplicação de código
- ✅ Manutenção centralizada
- ✅ Configuração mais clara e consistente

---

### 2. Decorators e Tasks

#### ❌ Antes (Prefect 1.4)
```python
from prefect import task

@task(checkpoint=False)
def get_mongodb_connection_string(secret_name: str) -> str:
    connection = get_secret(secret_name=secret_name, path="/taxirio")
    return connection[secret_name]
```

#### ✅ Agora (Prefect 3.0)
```python
from prefect import task

@task
def get_mongodb_connection_string(secret_name: str, secret_path: str = "/taxirio") -> str:
    """
    Obtém a string de conexão do MongoDB a partir do Infisical.
    
    Args:
        secret_name: Nome do secret no Infisical
        secret_path: Caminho do secret no Infisical
    
    Returns:
        String de conexão do MongoDB
    """
    utils.log("Obtendo string de conexão do MongoDB do Infisical")
    connection = get_secret(secret_name=secret_name, path=secret_path)
    return connection[secret_name]
```

**Mudanças:**
- ✅ Remoção do parâmetro `checkpoint` (não existe no Prefect 3.0)
- ✅ Adição de docstrings completas em todas as funções
- ✅ Logs mais descritivos

---

### 3. Logging

#### ❌ Antes (Prefect 1.4)
```python
from prefect import context

def log(message: str, level: str = "info") -> None:
    context.logger.log(levels[level], message)
```

#### ✅ Agora (Prefect 3.0)
```python
from prefect import get_run_logger

def log(message: str, level: str = "info") -> None:
    """Registra uma mensagem no logger do Prefect."""
    logger = get_run_logger()
    logger.log(levels[level], message)
```

**Mudanças:**
- ✅ Substituição de `context.logger` por `get_run_logger()`
- ✅ Compatibilidade com Prefect 3.0

---

### 4. State Handlers

#### ❌ Antes (Prefect 1.4)
```python
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

with Flow(
    name="IPLANRIO: cities - Dump da collection do MongoDB do TaxiRio",
    state_handlers=[handler_inject_bd_credentials],
    skip_if_running=True,
    parallelism=1,
) as rj_iplanrio__taxirio__cities__flow:
    ...
```

#### ✅ Agora (Prefect 3.0)
```python
from iplanrio.pipelines_utils.env import inject_bd_credentials_task

@flow(log_prints=True)
def rj_iplanrio__taxirio(table_id: str, ...):
    rename_current_flow_run_task(new_name=f"{table_id}")
    inject_bd_credentials_task(environment="prod")
    ...
```

**Mudanças:**
- ✅ `state_handlers` substituído por chamada explícita de task
- ✅ `rename_current_flow_run_task` para identificação clara
- ✅ Sintaxe de decorator `@flow` do Prefect 3.0

---

### 5. Schedules

#### ❌ Antes (Prefect 1.4)
```python
from datetime import datetime, timedelta
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pytz import timezone

def every_month(year: int, month: int, day: int) -> Schedule:
    return Schedule(
        clocks=[
            IntervalClock(
                interval=timedelta(days=30),
                start_date=datetime(year, month, day, 0, 0, 0, 
                                   tzinfo=timezone(Constants.TIMEZONE.value)),
                labels=[TaxiRio.RJ_TAXIRIO_AGENT_LABEL.value],
            ),
        ],
    )

# No flow:
rj_iplanrio__taxirio__cities__flow.schedule = every_month(2024, 9, 1)
```

#### ✅ Agora (Prefect 3.0)
```yaml
# No prefect.yaml
schedules:
  - interval: 2592000  # 30 dias em segundos
    anchor_date: "2024-09-01T00:00:00"
    timezone: America/Sao_Paulo
    slug: cities-monthly
    parameters:
      table_id: cities
```

**Mudanças:**
- ✅ Schedules declarativos no YAML ao invés de código Python
- ✅ Configuração mais clara e fácil de modificar
- ✅ Parâmetros explícitos para cada schedule
- ✅ Não precisa de funções auxiliares (every_month, every_week, etc.)

---

### 6. Storage e Run Config

#### ❌ Antes (Prefect 1.4)
```python
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import Constants

rj_iplanrio__taxirio__cities__flow.storage = GCS(Constants.GCS_FLOWS_BUCKET.value)

rj_iplanrio__taxirio__cities__flow.run_config = KubernetesRun(
    image=Constants.DOCKER_IMAGE.value,
    labels=[TaxiRio.RJ_TAXIRIO_AGENT_LABEL.value],
)
```

#### ✅ Agora (Prefect 3.0)
```yaml
# No prefect.yaml
build:
  - prefect_docker.deployments.steps.build_docker_image:
      id: build-image
      image_name: ghcr.io/prefeitura-rio/prefect_rj_iplanrio/deployments
      tag: "rj_iplanrio__taxirio-{{ get-commit-hash.stdout }}"
      dockerfile: pipelines/rj_iplanrio__taxirio/Dockerfile

push:
  - prefect_docker.deployments.steps.push_docker_image:
      image_name: "{{ build-image.image_name }}"
      tag: "{{ build-image.tag }}"

deployments:
  - name: rj-iplanrio--taxirio--prod
    work_pool:
      name: k3s-pool
      job_variables:
        image: "{{ build-image.image_name }}:{{ build-image.tag }}"
```

**Mudanças:**
- ✅ Build e push configurados declarativamente
- ✅ Versionamento automático via commit hash
- ✅ Separação clara entre staging e prod
- ✅ Não precisa definir storage/run_config em código

---

### 7. Parametrização do Flow

#### ❌ Antes (Prefect 1.4)
```python
from prefect import Parameter

path = Parameter("path", default="output")
dump_mode = Parameter("dump_mode", default="overwrite")
dataset_id = Parameter("dataset_id", default=TaxiRio.DATASET_ID.value)
table_id = Parameter("table_id", default=Cities.TABLE_ID.value)
```

**Problema**: Cada flow tinha seus próprios Parameters fixos para cada tabela

#### ✅ Agora (Prefect 3.0)
```python
@flow(log_prints=True)
def rj_iplanrio__taxirio(
    table_id: str,
    path: str = "output",
    dataset_id: str = Constants.DATASET_ID.value,
    dump_mode: Optional[str] = None,
    frequency: Optional[str] = None,
):
    config = TABLE_CONFIGS[table_id]
    dump_mode = dump_mode or config.dump_mode
    frequency = frequency or config.frequency
    ...
```

**Mudanças:**
- ✅ Parâmetros nativos do Python ao invés de objetos `Parameter`
- ✅ Um flow processa TODAS as tabelas
- ✅ Configurações padrão por tabela em `constants.py`
- ✅ Possibilidade de override via parâmetros

---

### 8. Configuração de Tabelas

#### ❌ Antes (Prefect 1.4)
```python
# Espalhado em vários arquivos constants.py
# cities/constants.py
class Constants(Enum):
    TABLE_ID = "cities"

# drivers/constants.py
class Constants(Enum):
    TABLE_ID = "drivers"

# races/constants.py
class Constants(Enum):
    TABLE_ID = "races"
```

#### ✅ Agora (Prefect 3.0)
```python
# constants.py centralizado
class TableConfig:
    def __init__(
        self,
        table_id: str,
        dump_mode: str = "overwrite",
        partition_cols: Optional[list[str]] = None,
        use_period: bool = False,
        frequency: Optional[str] = None,
    ):
        ...

TABLE_CONFIGS = {
    "cities": TableConfig(
        table_id="cities",
        dump_mode="overwrite",
        partition_cols=[],
        use_period=False,
    ),
    "races": TableConfig(
        table_id="races",
        dump_mode="append",
        partition_cols=["ano_particao", "mes_particao", "dia_particao"],
        use_period=True,
        frequency="D",
    ),
    ...
}
```

**Mudanças:**
- ✅ Configuração centralizada em um único lugar
- ✅ Estrutura de dados tipada (TableConfig)
- ✅ Todas as configurações de uma tabela juntas
- ✅ Fácil de adicionar novas tabelas

---

### 9. Detecção Dinâmica de Schema

#### ✅ Novo no Prefect 3.0
```python
# O flow detecta automaticamente se a tabela usa pipeline ou generate_pipeline
mongodb_module = importlib.import_module(f"pipelines.rj_iplanrio__taxirio.{table_id}.mongodb")
schema = mongodb_module.schema

if config.use_period:
    # Para tabelas grandes com processamento por período
    generate_pipeline = mongodb_module.generate_pipeline
    data_path = dump_collection_from_mongodb_per_period(...)
else:
    # Para tabelas com dump completo
    pipeline = mongodb_module.pipeline
    data_path = dump_collection_from_mongodb(...)
```

**Benefícios:**
- ✅ Importação dinâmica de schemas baseada em `table_id`
- ✅ Detecção automática do tipo de processamento
- ✅ Um flow serve para todos os tipos de tabela

---

## Estrutura de Arquivos Detalhada

### Arquivos Principais

| Arquivo | Propósito | Migrado de |
|---------|-----------|------------|
| `flow.py` | Flow unificado | 9 arquivos `flows.py` separados |
| `tasks.py` | Tasks reutilizáveis | `taxirio/tasks.py` |
| `constants.py` | Configurações centralizadas | 9 arquivos `constants.py` + `taxirio/constants.py` |
| `prefect.yaml` | Deployments e schedules | `schedules.py` + configurações de flow |
| `Dockerfile` | Imagem Docker | Novo (Prefect 3.0) |
| `pyproject.toml` | Dependências | Novo (Prefect 3.0) |
| `README.md` | Documentação de uso | Novo |
| `MIGRATION.md` | Este documento | Novo |

### Schemas MongoDB (por tabela)

Cada tabela mantém seu próprio `mongodb.py` com:
- `schema`: Schema PyArrow da collection
- `pipeline` OU `generate_pipeline()`: Pipeline de agregação MongoDB

| Tabela | Schema | Tipo de Pipeline |
|--------|--------|------------------|
| cities | `cities/mongodb.py` | Estático |
| drivers | `drivers/mongodb.py` | Estático |
| users | `users/mongodb.py` | Estático |
| discounts | `discounts/mongodb.py` | Estático |
| paymentmethods | `paymentmethods/mongodb.py` | Estático |
| rankingraces | `rankingraces/mongodb.py` | Estático |
| metricsdriverunoccupieds | `metricsdriverunoccupieds/mongodb.py` | Estático |
| races | `races/mongodb.py` | Dinâmico (função) |
| passengers | `passengers/mongodb.py` | Dinâmico (função) |

---

## Tabelas e Configurações

### Resumo de Schedules

| Tabela | Frequência | Horário | Dump Mode | Particionamento |
|--------|-----------|---------|-----------|-----------------|
| cities | Mensal | 00:00 | overwrite | Não |
| drivers | Mensal | 00:00 | overwrite | ano, mês |
| discounts | Mensal | 00:00 | overwrite | Não |
| paymentmethods | Mensal | 00:00 | overwrite | Não |
| rankingraces | Mensal | 00:00 | overwrite | Não |
| metricsdriverunoccupieds | Mensal | 00:00 | overwrite | Não |
| users | Semanal | 00:00 | overwrite | ano, mês |
| races | Diário | 01:00 | append | ano, mês, dia |
| passengers | Diário | 01:00 | append | ano, mês, dia |

---

## Como Adicionar Nova Tabela

1. **Criar Schema MongoDB**

```bash
mkdir pipelines/rj_iplanrio__taxirio/nova_tabela
touch pipelines/rj_iplanrio__taxirio/nova_tabela/__init__.py
```

2. **Criar `nova_tabela/mongodb.py`**

```python
from pyarrow import string
from pymongoarrow.api import Schema

pipeline = [
    {"$project": {...}},
    {"$unset": "_id"},
]

schema = Schema({...})
```

3. **Adicionar em `constants.py`**

```python
TABLE_CONFIGS = {
    ...
    "nova_tabela": TableConfig(
        table_id="nova_tabela",
        dump_mode="overwrite",
        partition_cols=[],
        use_period=False,
    ),
}
```

4. **Adicionar Schedule em `prefect.yaml`**

```yaml
- interval: 86400
  anchor_date: "2024-01-01T00:00:00"
  timezone: America/Sao_Paulo
  slug: nova-tabela-daily
  parameters:
    table_id: nova_tabela
```

---

## Testes Recomendados

### 1. Teste Local (sem schedule)
```bash
# Testar uma tabela específica
prefect deployment run rj-iplanrio--taxirio--staging \
  --param table_id=cities
```

### 2. Teste de Todas as Tabelas
```bash
for table in cities drivers races passengers users discounts paymentmethods rankingraces metricsdriverunoccupieds; do
  echo "Testando $table..."
  prefect deployment run rj-iplanrio--taxirio--staging --param table_id=$table
done
```

### 3. Verificar Schedules
```bash
prefect deployment inspect rj-iplanrio--taxirio--prod
```

---

## Dependências Adicionadas

### MongoDB
- `pymongo>=4.0.0`: Cliente MongoDB
- `pymongoarrow>=1.0.0`: Conversão eficiente MongoDB → Arrow
- `pyarrow>=10.0.0`: Manipulação de dados Arrow

### Prefect
- `prefect==3.4.9`: Framework Prefect 3.0
- `prefect-docker>=0.6.5`: Integração Docker

---

## Checklist de Migração

- ✅ Flow unificado criado (`flow.py`)
- ✅ Tasks migradas para Prefect 3.0 (`tasks.py`)
- ✅ Constants consolidados (`constants.py`)
- ✅ Schedules migrados para YAML (`prefect.yaml`)
- ✅ Schemas MongoDB copiados (9 tabelas)
- ✅ Dockerfile criado
- ✅ pyproject.toml configurado
- ✅ Documentação completa (README.md, MIGRATION.md)
- ✅ Logging adaptado (`prefect.context` → `get_run_logger`)
- ✅ State handlers convertidos em tasks
- ✅ Utils.py migrado para Prefect 3.0

---

## Contato e Manutenção

Para questões sobre esta migração, consulte:
- **Repositório**: https://github.com/prefeitura-rio/prefect_rj_iplanrio
- **Documentação Prefect 3.0**: https://docs.prefect.io/3.0/
- **Issues**: https://github.com/prefeitura-rio/prefect_rj_iplanrio/issues

---

**Migração concluída em**: 2026-05-03  
**Versão Prefect**: 3.4.9  
**Status**: ✅ Pronto para deploy
