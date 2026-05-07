# Pipeline TaxiRio - Migração Prefect 1.4 → 3.0

## Visão Geral

Pipeline migrada do Prefect 1.4 para Prefect 3.0 que realiza dump de collections do MongoDB do TaxiRio para o BigQuery.

**Características principais:**
- ✅ Flow unificado que processa todas as tabelas do TaxiRio
- ✅ Configuração parametrizada via `prefect.yaml`
- ✅ Suporte a dump completo (overwrite) e incremental (append)
- ✅ Processamento por período para tabelas grandes
- ✅ Particionamento automático no BigQuery

## Tabelas Processadas

### Dumps Mensais (Overwrite)
- **cities**: Cidades cadastradas no sistema
- **drivers**: Motoristas cadastrados
- **discounts**: Descontos disponíveis
- **paymentmethods**: Métodos de pagamento
- **rankingraces**: Ranking de corridas
- **metricsdriverunoccupieds**: Métricas de motoristas desocupados

### Dumps Semanais (Overwrite)
- **users**: Usuários do sistema

### Dumps Diários (Append)
- **races**: Corridas realizadas (particionado por ano/mês/dia)
- **passengers**: Passageiros (particionado por ano/mês/dia)

## Estrutura do Projeto

```
rj_iplanrio__taxirio/
├── flow.py                    # Flow unificado para todas as tabelas
├── tasks.py                   # Tasks reutilizáveis para MongoDB e BigQuery
├── constants.py               # Configurações de cada tabela
├── prefect.yaml              # Configuração de deployments e schedules
├── Dockerfile                # Imagem Docker da pipeline
├── pyproject.toml            # Dependências do projeto
├── README.md                 # Esta documentação
│
├── cities/                   # Schema e pipeline da tabela cities
│   └── mongodb.py
├── drivers/                  # Schema e pipeline da tabela drivers
│   └── mongodb.py
├── races/                    # Schema e pipeline da tabela races
│   └── mongodb.py
├── passengers/               # Schema e pipeline da tabela passengers
│   └── mongodb.py
├── users/                    # Schema e pipeline da tabela users
│   └── mongodb.py
├── discounts/                # Schema e pipeline da tabela discounts
│   └── mongodb.py
├── paymentmethods/           # Schema e pipeline da tabela paymentmethods
│   └── mongodb.py
├── rankingraces/             # Schema e pipeline da tabela rankingraces
│   └── mongodb.py
└── metricsdriverunoccupieds/ # Schema e pipeline da tabela metricsdriverunoccupieds
    └── mongodb.py
```

## Principais Mudanças da Migração

### Prefect 1.4 → Prefect 3.0

1. **Consolidação de Flows**
   - ❌ Antes: 9 flows separados, um para cada tabela
   - ✅ Agora: 1 flow unificado parametrizado

2. **Configuração de Schedules**
   - ❌ Antes: Schedules definidos em código Python
   - ✅ Agora: Schedules definidos no `prefect.yaml` com sintaxe declarativa

3. **Decorators e Sintaxe**
   - ❌ Antes: `@task(checkpoint=False)`
   - ✅ Agora: `@task` (sintaxe simplificada do Prefect 3.0)

4. **State Handlers**
   - ❌ Antes: `state_handlers=[handler_inject_bd_credentials]`
   - ✅ Agora: `inject_bd_credentials_task()` como task explícita

5. **Storage e Run Config**
   - ❌ Antes: GCS Storage + KubernetesRun definidos em código
   - ✅ Agora: Configuração Docker no `prefect.yaml`

## Como Usar

### Executar Localmente

```bash
# Processar uma tabela específica
prefect deployment run rj-iplanrio--taxirio--staging --param table_id=cities

# Processar com parâmetros customizados
prefect deployment run rj-iplanrio--taxirio--staging \
  --param table_id=races \
  --param dump_mode=append \
  --param frequency=D
```

### Adicionar Nova Tabela

1. **Criar schema MongoDB** em `<table_name>/mongodb.py`:

```python
from pyarrow import string
from pymongoarrow.api import Schema

# Para tabelas simples (sem período)
pipeline = [
    {"$project": {...}},
    {"$unset": "_id"},
]

# OU para tabelas com período
def generate_pipeline(start, end):
    return [
        {"$match": {"createdAt": {"$gte": start, "$lt": end}}},
        {"$project": {...}},
        {"$unset": "_id"},
    ]

schema = Schema({...})
```

2. **Adicionar configuração** em `constants.py`:

```python
TABLE_CONFIGS = {
    ...
    "nova_tabela": TableConfig(
        table_id="nova_tabela",
        dump_mode="overwrite",  # ou "append"
        partition_cols=["ano_particao", "mes_particao"],
        use_period=False,  # ou True
        frequency=None,  # ou "D" para diário
    ),
}
```

3. **Adicionar schedule** em `prefect.yaml`:

```yaml
- interval: 86400  # segundos
  anchor_date: "2024-01-01T00:00:00"
  timezone: America/Sao_Paulo
  slug: nova-tabela-daily
  parameters:
    table_id: nova_tabela
```

## Configuração de Tabelas

Cada tabela é configurada através da classe `TableConfig`:

- **table_id**: Nome da collection no MongoDB
- **dump_mode**: 
  - `overwrite`: Substitui todos os dados
  - `append`: Adiciona novos dados incrementalmente
- **partition_cols**: Colunas para particionamento no BigQuery
- **use_period**: Se deve processar em períodos (útil para tabelas grandes)
- **frequency**: Frequência de quebra por período (`D`=diário, `M`=mensal)

## Schedules

Os schedules são configurados no `prefect.yaml`:

- **Mensal** (2592000s): cities, drivers, discounts, paymentmethods, rankingraces, metricsdriverunoccupieds
- **Semanal** (604800s): users
- **Diário** (86400s): races, passengers

## Dependências

Principais dependências do projeto:

- `prefect>=3.4.9`: Framework de orquestração
- `pymongo>=4.0.0`: Client MongoDB
- `pymongoarrow>=1.0.0`: Conversão eficiente MongoDB → Arrow
- `pyarrow>=10.0.0`: Manipulação de dados em formato Arrow
- `prefeitura-rio`: Utilitários da Prefeitura do Rio

## Secrets Necessários

A pipeline requer os seguintes secrets no Infisical:

- **Path**: `/taxirio`
- **Secret**: `DB_CONNECTION_STRING`
  - String de conexão MongoDB (formato: `mongodb://user:pass@host:port/database`)

## Observações

- O flow detecta automaticamente se a tabela usa pipeline estático ou dinâmico
- Tabelas com `use_period=True` são processadas em lotes por data
- O particionamento no BigQuery é automático baseado em `partition_cols`
- Logs detalhados são gerados em cada etapa do processamento

## Autores

Migração realizada em 2026-05-03 do repositório original:
- **Repositório Antigo**: https://github.com/prefeitura-rio/pipelines_rj_iplanrio/tree/main/pipelines/taxirio
- **Prefect**: 1.4 → 3.0
