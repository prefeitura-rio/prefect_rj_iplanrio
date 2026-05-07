# Pipeline de Atividade Econômica - SMFP

Pipeline unificado para dump de tabelas do banco de dados **DW_BI_ALVARAS** (Alvarás) da Secretaria Municipal de Fazenda e Planejamento (SMFP) para o BigQuery.

## 📋 Descrição

Este pipeline foi migrado do Prefect 1.4 para o Prefect 3.0, consolidando múltiplos flows em um único flow parametrizável que processa 10 tabelas diferentes do sistema de Alvarás.

### Tabelas Processadas

#### Tabelas de Fatos (FACT)
- **fact_fatoalvaras**: Fato principal de Alvarás com dados quantitativos e relacionamentos
- **fact_fatocp**: Fato de Consultas Prévias

#### Tabelas Dimensão (TAB)
- **tab_alvara**: Informações descritivas dos Alvarás
- **tab_atvprocesso**: Atividades de Processo
- **tab_cae**: CAE (Código de Atividade Econômica)
- **tab_cnae**: CNAE (Classificação Nacional de Atividades Econômicas)
- **tab_consulta**: Consultas Prévias
- **tab_direcionamento**: Direcionamento de processos
- **tab_tipocontribuinte_tipocontribuint**: Tipos de Contribuinte
- **tab_tiposolicitacao**: Tipos de Solicitação

## 🏗️ Arquitetura

### Estrutura de Arquivos

```
rj_smfp__atividade_economica/
├── flow.py              # Flow unificado principal
├── constants.py         # Configurações das tabelas e queries
├── prefect.yaml         # Deployments e schedules
├── Dockerfile           # Build da imagem Docker
├── pyproject.toml       # Dependências do projeto
├── .dockerignore        # Arquivos ignorados no build
└── README.md            # Esta documentação
```

### Flow Unificado

O flow `rj_smfp__atividade_economica` processa qualquer tabela baseado no parâmetro `table_id`:

```python
from pipelines.rj_smfp__atividade_economica.flow import rj_smfp__atividade_economica

# Processar tabela específica
rj_smfp__atividade_economica(table_id="fact_fatoalvaras")
```

## 🚀 Uso

### Executar Localmente

```bash
# Processar uma tabela específica
python -c "from pipelines.rj_smfp__atividade_economica.flow import rj_smfp__atividade_economica; rj_smfp__atividade_economica(table_id='fact_fatoalvaras')"
```

### Deploy no Prefect Cloud

```bash
# Build e deploy de todos os deployments
prefect deploy --all

# Deploy específico
prefect deploy -n rj-smfp--atividade-economica--prod
```

## ⏱️ Schedules

Todas as tabelas são processadas **diariamente às 02:00 AM (horário de São Paulo)**, com intervalos de 10 minutos entre cada tabela:

| Tabela | Horário (America/Sao_Paulo) |
|--------|----------------------------|
| fact_fatoalvaras | 02:00 |
| fact_fatocp | 02:10 |
| tab_alvara | 02:20 |
| tab_atvprocesso | 02:30 |
| tab_cae | 02:40 |
| tab_cnae | 02:50 |
| tab_consulta | 03:00 |
| tab_direcionamento | 03:10 |
| tab_tipocontribuinte_tipocontribuint | 03:20 |
| tab_tiposolicitacao | 03:30 |

## 🔧 Configuração

### Banco de Dados

- **Tipo**: SQL Server
- **Host**: 10.70.15.11
- **Porta**: 1433
- **Database**: DW_BI_ALVARAS
- **Credenciais**: Armazenadas no Infisical (`/db-alvaras`)

### BigQuery

- **Dataset**: `atividade_economica`
- **Modo de dump**: `overwrite` (padrão)
- **BigLake**: Habilitado para todas as tabelas

## 📝 Principais Mudanças da Migração

### Prefect 1.4 → 3.0

1. **Flow unificado**: Antes havia flows separados, agora um único flow processa todas as tabelas
2. **Configuração centralizada**: `constants.py` com dataclasses ao invés de dicionários
3. **Deployment moderno**: `prefect.yaml` ao invés de `schedules.py`
4. **Docker build**: Integração nativa com GitHub Container Registry
5. **Type hints**: Tipos completos em todas as funções
6. **Docstrings**: Documentação detalhada em todos os módulos

## 🔐 Secrets

As credenciais do banco de dados são obtidas do Infisical:

- **Secret path**: `/db-alvaras`
- **Campos esperados**:
  - `DB_USERNAME`: Usuário do SQL Server
  - `DB_PASSWORD`: Senha do SQL Server

## 📊 Monitoramento

O pipeline adiciona automaticamente:
- ✅ Timestamp de execução em cada registro
- ✅ Logs detalhados de progresso
- ✅ Retry automático em caso de falha (2 tentativas)
- ✅ Batches de 50.000 registros para processamento eficiente

## 🤝 Contribuindo

Para adicionar novas tabelas:

1. Adicione a configuração em `constants.py`:
```python
TABLE_CONFIGS = {
    # ...
    "nova_tabela": TableConfig(
        table_id="nova_tabela",
        execute_query="SELECT * FROM ...",
    ),
}
```

2. Adicione o schedule em `prefect.yaml`:
```yaml
- interval: 86400
  anchor_date: "2022-03-21T03:40:00"
  timezone: America/Sao_Paulo
  slug: nova-tabela-daily
  parameters:
    table_id: nova_tabela
```

## 📚 Referências

- [Documentação Prefect 3.0](https://docs.prefect.io)
- [Pipeline Original (Prefect 1.4)](https://github.com/prefeitura-rio/pipelines_rj_smfp/tree/main/pipelines/atividade_economica/dump_db)
- [Guia de Migração Prefect 1.4 → 3.0](.claude/skills/SKILL.md)
