# Pipeline Força Municipal (Guarda Municipal RJ)

Pipeline para extração de dados da API CIVITAS/CORIO (Sistema HxGN OnCall) e carga no BigQuery.

## 📋 Endpoints Disponíveis

A pipeline suporta 11 endpoints GET organizados em 3 categorias:

### 📍 Unidades/Viaturas (3 endpoints)
| table_id | Endpoint | Descrição |
|----------|----------|-----------|
| `unidades_ativas` | `/unidades/ativas` | Viaturas logadas no sistema (status atual) |
| `unidades_historico` | `/unidades/historico` | Histórico completo de todas as viaturas |
| `unit_positions` | `/unit/positions` | Posições geográficas atualizadas das unidades |

### 🚨 Ocorrências/Eventos (3 endpoints)
| table_id | Endpoint | Descrição |
|----------|----------|-----------|
| `ocorrencias_ativas` | `/ocorrencias/ativas` | Ocorrências em curso do sistema |
| `ocorrencias_historico` | `/ocorrencias/historico` | Histórico completo de todas as ocorrências |
| `ocorrencias_ativas_v2` | `/ocorrencias/ativas/v2` | Ocorrências ativas (versão 2 com campos adicionais) |

### 📊 QMD - Quadro de Movimentação Diária (5 endpoints)
| table_id | Endpoint | Descrição |
|----------|----------|-----------|
| `qmd` | `/qmd` | Todos os registros do QMD |
| `qmd_ativos` | `/qmd/ativos` | Apenas registros ativos do QMD |
| `qmd_servicos` | `/qmd/servicos` | Serviços cadastrados no QMD |
| `qmd_missoes` | `/qmd/missoes` | Missões planejadas/em andamento |
| `qmd_plano` | `/qmd/plano` | Planos operacionais cadastrados |

## 🚀 Como Usar

### 1. Executar Pipeline via Python

```python
from pipelines.rj_segur__forca_municipal.flow import rj_segur__forca_municipal

# Exemplo 1: Extrair unidades ativas (usa mapeamento automático)
rj_segur__forca_municipal(table_id="unidades_ativas")

# Exemplo 2: Extrair histórico de ocorrências
rj_segur__forca_municipal(
    table_id="ocorrencias_historico",
    dataset_id="forca_municipal"
)

# Exemplo 3: Extrair com endpoint customizado
rj_segur__forca_municipal(
    table_id="qmd_ativos",
    endpoint="/qmd/custom"  # Sobrescreve o mapeamento padrão
)
```

### 2. Executar via Prefect CLI

```bash
# Exemplo básico
prefect deployment run rj-segur--forca-municipal/production \
  --param table_id=unidades_ativas

# Com parâmetros adicionais
prefect deployment run rj-segur--forca-municipal/production \
  --param table_id=ocorrencias_historico \
  --param dataset_id=forca_municipal \
  --param dump_mode=append
```

### 3. Listar Endpoints Disponíveis

```python
from pipelines.rj_segur__forca_municipal.constants import EndpointConfig

# Listar todos os endpoints
all_endpoints = EndpointConfig.list_all()
for table_id, info in all_endpoints.items():
    print(f"{table_id}: {info['endpoint']} - {info['description']}")

# Listar por categoria
unidades = EndpointConfig.list_by_category("Unidades/Viaturas")
print(unidades)

# Obter endpoint específico
endpoint = EndpointConfig.get_endpoint("unidades_ativas")
print(endpoint)  # /unidades/ativas
```

## 📁 Estrutura de Arquivos

```
pipelines/rj_segur__forca_municipal/
├── constants.py       # Mapeamento de table_id → endpoint + configurações da API
├── flow.py           # Flow principal do Prefect
├── task.py           # Tasks de extração de dados da API
└── README.md         # Este arquivo
```

## 🔧 Configuração

### Arquivo `constants.py`

Contém:
- **EndpointConfig**: Classe com mapeamento de `table_id` para `endpoint`
- **API_CONFIG**: Configurações de autenticação e conexão
- **PAGINATION_CONFIG**: Configurações padrão de paginação

### Adicionar Novo Endpoint

Para adicionar um novo endpoint, edite `constants.py`:

```python
ENDPOINTS: Dict[str, str] = {
    # ... endpoints existentes ...
    "novo_endpoint": "/novo/path",  # Adicione aqui
}

DESCRIPTIONS: Dict[str, str] = {
    # ... descrições existentes ...
    "novo_endpoint": "Descrição do novo endpoint",
}

CATEGORIES: Dict[str, str] = {
    # ... categorias existentes ...
    "novo_endpoint": "Categoria",
}
```

## ⚙️ Parâmetros do Flow

| Parâmetro | Tipo | Obrigatório | Padrão | Descrição |
|-----------|------|-------------|--------|-----------|
| `table_id` | str | Sim | - | ID da tabela (identifica o endpoint) |
| `dataset_id` | str | Não | `"forca_municipal"` | ID do dataset no BigQuery |
| `endpoint` | str | Não | `None` | Endpoint customizado (sobrescreve mapeamento) |
| `dump_mode` | str | Não | `"overwrite"` | Modo de escrita (`overwrite` ou `append`) |
| `biglake_table` | bool | Não | `True` | Criar tabela BigLake |

## 🔐 Autenticação

A pipeline usa autenticação configurada em `constants.py`:

```python
API_CONFIG = {
    "base_url": "https://data.corio-oncall.com.br:8086/api",
    "username": "RestAPI",
    "password": "@Hexagon2024",
    "proxy": "http://proxy.squirrel-regulus.ts.net:3128",
    "verify_ssl": False,
    "timeout": 30,
}
```

## 📊 Dados Extraídos

Os dados são:
1. Extraídos da API com paginação automática
2. Convertidos para DataFrame do Pandas
3. Colunas JSON são expandidas automaticamente
4. Salvos como CSV temporário
5. Carregados no BigQuery via GCS

## 🔍 Troubleshooting

### Erro: table_id não encontrado

```python
ValueError: table_id 'xyz' não encontrado. Disponíveis: unidades_ativas, ...
```

**Solução**: Use um `table_id` válido da lista de endpoints disponíveis.

### Erro: Falha na autenticação

```
ERROR - Erro HTTP ao autenticar: 401
```

**Solução**: Verifique as credenciais em `constants.py`.

### Erro: Timeout

```
ERROR - Timeout ao requisitar endpoint
```

**Solução**: Aumente o `timeout` em `constants.py` ou verifique conectividade com o proxy.

## 📝 Exemplos de Uso Completo

### Exemplo 1: Extrair todos os endpoints de Unidades

```python
from pipelines.rj_segur__forca_municipal.constants import EndpointConfig
from pipelines.rj_segur__forca_municipal.flow import rj_segur__forca_municipal

# Lista endpoints de Unidades
unidades = EndpointConfig.list_by_category("Unidades/Viaturas")

# Extrai cada um
for table_id in unidades.keys():
    print(f"Extraindo {table_id}...")
    rj_segur__forca_municipal(table_id=table_id)
```

### Exemplo 2: Extrair com append mode

```python
# Útil para dados incrementais
rj_segur__forca_municipal(
    table_id="ocorrencias_historico",
    dump_mode="append"
)
```

### Exemplo 3: Extrair para dataset customizado

```python
rj_segur__forca_municipal(
    table_id="qmd_ativos",
    dataset_id="seguranca_publica",
    biglake_table=False
)
```

## 🆘 Suporte

- **Documentação da API**: Ver `api.pdf` no diretório raiz
- **Issues**: Reportar problemas via GitHub/Jira
- **Contato**: Equipe IplanRio
