# Pipeline: COR - Meteorologia REDEMET

## Descrição

Pipeline para coleta automatizada de dados meteorológicos das estações do REDEMET (Rede de Meteorologia do Comando da Aeronáutica) localizadas no município do Rio de Janeiro.

## Migração Prefect 1.4 → 3.0

Esta pipeline foi migrada do Prefect 1.4 para o Prefect 3.0 mantendo toda a funcionalidade original.

### Repositório Original

- **Repositório:** https://github.com/prefeitura-rio/pipelines_rj_cor
- **Caminho:** `pipelines/meteorologia/meteorologia_redemet`
- **Branch:** main

### Dois Flows Principais

Esta pipeline possui **dois flows independentes**:

#### 1. Flow de Dados Meteorológicos (Horário)
- **Nome:** `rj_cor__meteorologia_redemet`
- **Frequência:** A cada 1 hora
- **Função:** Coleta dados meteorológicos das 5 estações
- **Tabela:** `clima_estacao_meteorologica.meteorologia_redemet`

#### 2. Flow de Atualização de Estações (Mensal)
- **Nome:** `rj_cor__meteorologia_redemet_estacoes`
- **Frequência:** A cada 30 dias
- **Função:** Atualiza cadastro de estações e verifica novas estações
- **Tabela:** `clima_estacao_meteorologica.estacoes_redemet`

### Dados Coletados

#### Estações Meteorológicas (Aeródromos)

| Código | Nome | Tipo |
|--------|------|------|
| SBAF | Campo dos Afonsos | Aeródromo Militar |
| SBGL | Galeão - Tom Jobim | Aeroporto Internacional |
| SBJR | Jacarepaguá | Aeródromo |
| SBRJ | Santos Dumont | Aeroporto |
| SBSC | Santa Cruz | Base Aérea |

#### Variáveis Coletadas (Dados Meteorológicos)

- **Temperatura:** Temperatura do ar em °C
- **Umidade:** Umidade relativa do ar em %
- **Condições do tempo:** Descrição textual das condições
- **Céu:** Estado do céu (limpo, nublado, etc.)
- **Teto:** Altura da base das nuvens
- **Visibilidade:** Visibilidade horizontal

#### Informações de Estações

- **ID da estação:** Código ICAO do aeródromo
- **Nome:** Nome da estação/aeródromo
- **Latitude e Longitude:** Coordenadas geográficas
- **Altitude:** Altitude em metros
- **Data de atualização:** Data da última atualização do cadastro

## Estrutura

```
rj_cor__meteorologia_redemet/
├── flow.py              # Dois flows principais orquestrando as tasks
├── tasks.py             # Tasks de coleta, transformação e salvamento
├── utils.py             # Funções auxiliares de particionamento
├── prefect.yaml         # Configuração de deployments e schedules
├── Dockerfile           # Build da imagem Docker
├── pyproject.toml       # Dependências do projeto
├── README.md            # Esta documentação
└── __init__.py          # Inicialização do pacote
```

## BigQuery

### Tabela de Dados Meteorológicos
- **Dataset:** `clima_estacao_meteorologica`
- **Tabela:** `meteorologia_redemet`
- **Modo:** Append (adiciona novos dados)
- **Particionamento:** Por ano, mês e dia (da data_medicao)

### Tabela de Estações
- **Dataset:** `clima_estacao_meteorologica`
- **Tabela:** `estacoes_redemet`
- **Modo:** Overwrite (substitui dados completos)
- **Particionamento:** Por data_atualizacao

## Schedules

### Dados Meteorológicos
- **Frequência:** A cada 1 hora (3600 segundos)
- **Timezone:** America/Sao_Paulo
- **Horário inicial:** 00:10:00 (mantido do schedule original)
- **Deployment:** `rj-cor--meteorologia-redemet--prod`

### Atualização de Estações
- **Frequência:** A cada 30 dias (2592000 segundos)
- **Timezone:** America/Sao_Paulo
- **Horário inicial:** 00:12:00 (mantido do schedule original)
- **Deployment:** `rj-cor--meteorologia-redemet-estacoes--prod`

## Principais Mudanças

### 1. Estrutura de Tasks

**Prefect 1.4:**
```python
# tasks.py
from prefect import task

@task
def get_dates(first_date, last_date):
    # ...
```

**Prefect 3.0:**
```python
# tasks.py
from prefect import task

@task
def get_dates_task(first_date: str, last_date: str) -> Tuple[str, str, bool]:
    """
    Documentação completa com tipos.
    """
    # ...
```

### 2. Dois Flows Separados

**Prefect 1.4:**
```python
# flows.py - dois flows em contextos separados
with Flow(name="COR: Meteorologia - Meteorologia REDEMET") as flow1:
    # ...

with Flow(name="COR: Meteorologia - Meteorologia REDEMET - Atualização das estações") as flow2:
    # ...
```

**Prefect 3.0:**
```python
# flow.py - dois flows como funções decoradas
@flow(log_prints=True)
def rj_cor__meteorologia_redemet(...):
    """Flow de dados meteorológicos."""
    # ...

@flow(log_prints=True)
def rj_cor__meteorologia_redemet_estacoes(...):
    """Flow de atualização de estações."""
    # ...
```

### 3. Particionamento de Dados

**Prefect 1.4:**
```python
# Particionamento por data_medicao ou data_atualizacao
dataframe, partitions = parse_date_columns(dataframe, partition_column)
```

**Prefect 3.0:**
```python
# Mesmo padrão, mas com funções utilitárias melhoradas
# Particionamento: ano=YYYY/mes=MM/dia=DD/dados.csv
save_data_to_partitions_task(dataframe=dataframe, partition_column="data_medicao")
```

### 4. Verificação de Novas Estações

**Prefect 1.4:**
```python
# Lança erro se novas estações forem detectadas
raise ENDRUN(state=Failed(message))
```

**Prefect 3.0:**
```python
# Apenas loga aviso para atualização manual
print(f"⚠️  Nova(s) estação(ões) identificada(s): {new_stations}")
```

## Execução

### Executar manualmente (teste)

```bash
# Flow de dados meteorológicos
prefect deployment run rj-cor--meteorologia-redemet--staging

# Flow de atualização de estações
prefect deployment run rj-cor--meteorologia-redemet-estacoes--staging
```

### Executar com parâmetros customizados

```bash
# Backfill de dados meteorológicos
prefect deployment run rj-cor--meteorologia-redemet--staging \
  --param first_date="2026-01-01" \
  --param last_date="2026-01-31"
```

### Teste local

```python
from pipelines.rj_cor__meteorologia_redemet.flow import (
    rj_cor__meteorologia_redemet,
    rj_cor__meteorologia_redemet_estacoes
)

# Teste de dados meteorológicos
rj_cor__meteorologia_redemet(
    first_date="2026-05-25",
    last_date="2026-05-25"
)

# Teste de atualização de estações
rj_cor__meteorologia_redemet_estacoes()
```

## Dependências

- `prefect>=3.0.0` - Orquestração de workflows
- `pandas>=2.0.0` - Manipulação de dados
- `pendulum>=3.0.0` - Manipulação de datas e timezones
- `requests>=2.31.0` - Requisições HTTP para API do REDEMET
- `unidecode>=1.3.0` - Remoção de acentuação nos nomes de estações

## Variáveis de Ambiente / Secrets

### Infisical

As credenciais são obtidas via variável de ambiente (em produção usar Infisical):

- **REDEMET_TOKEN**: Token de autenticação da API do REDEMET
  - Obter em: https://api-redemet.decea.mil.br
  - Path (produção): `/`
  - Secret name: `REDEMET_TOKEN`

## Monitoramento

### Logs

Os logs podem ser visualizados em:
- Prefect UI: Flow Runs → Selecionar run → Logs
- CLI: `prefect flow-run logs <flow-run-id>`

### Alertas

A pipeline loga mensagens importantes:
- ✅ Total de registros coletados
- ✅ Hora mínima e máxima com dados
- ⚠️ Problemas ao coletar dados de estações específicas
- ⚠️ Novas estações detectadas (requer atualização manual do código)

## Troubleshooting

### Erro: "Problema na requisição"

**Causa:** API do REDEMET pode estar indisponível ou token inválido.

**Solução:**
1. Verificar status da API REDEMET
2. Validar token no Infisical/variável de ambiente
3. Atualizar token se necessário

### Erro: "Token inválido"

**Causa:** Token da API REDEMET expirado ou incorreto.

**Solução:**
1. Verificar variável de ambiente `REDEMET_TOKEN`
2. Obter novo token em https://api-redemet.decea.mil.br
3. Atualizar secret no Infisical (produção)

### Aviso: "Nova(s) estação(ões) identificada(s)"

**Causa:** O REDEMET adicionou novas estações no Rio de Janeiro.

**Solução:**
1. Verificar quais estações foram adicionadas nos logs
2. Atualizar lista de estações em `tasks.py` (função `download_meteorological_data_task`)
3. Adicionar novas estações na lista `rj_stations`
4. Testar e fazer deploy da atualização

### Dados não aparecem no BigQuery

**Causa:** Possível erro no upload ou transformação.

**Solução:**
1. Verificar logs do flow run
2. Verificar se os dados foram salvos em `/tmp/meteorologia_redemet/`
3. Verificar permissões do BigQuery

## Diferenças vs INMET

| Aspecto | REDEMET | INMET |
|---------|---------|-------|
| **Fonte** | DECEA (Aeronáutica) | INMET (Meteorologia) |
| **Estações** | 5 (aeródromos) | 9 (estações terrestres) |
| **Tipo** | Dados aeronáuticos | Dados meteorológicos gerais |
| **API** | api-redemet.decea.mil.br | apitempo.inmet.gov.br |
| **Variáveis** | 6 variáveis | 20 variáveis |
| **Flows** | 2 (dados + estações) | 1 (apenas dados) |
| **Schedule** | Horário + Mensal | Apenas Horário |
| **Particionamento** | ano/mes/dia | data=YYYY-MM-DD |

## Contato

Para dúvidas ou problemas:
- **Equipe:** IPLANRio
- **Projeto:** Prefect RJ IPLANRio
- **Repositório:** https://github.com/prefeitura-rio/prefect_rj_iplanrio

## Licença

Esta pipeline faz parte do projeto Prefect RJ IPLANRio da Prefeitura do Rio de Janeiro.
