# Pipeline SECONSERVA - Infraestrutura SISCOR Obras

Pipeline unificada para ingestão de dados do banco SISCOR (Sistema de Conservação e Obras) da Secretaria de Conservação (SECONSERVA).

## 📋 Descrição

Este flow extrai dados do banco SQL Server SISCOR e carrega no BigQuery. O SISCOR é responsável por gerenciar processos de autorização de obras públicas e privadas, incluindo informações sobre:

- Requerentes e executores de obras
- Localização e endereçamento
- Pareceres técnicos
- Situação e status dos processos
- Tipos de obras e natureza
- Licenças e autorizações

## 🗂️ Estrutura

```
rj_seconserva__infraestrutura_siscor_obras/
├── __init__.py           # Inicialização do pacote
├── constants.py          # Configurações e queries SQL
├── flow.py              # Flow unificado Prefect 3.0
├── prefect.yaml         # Configuração de deployments e schedules
├── Dockerfile           # Container para execução
├── pyproject.toml       # Dependências do projeto
└── README.md            # Esta documentação
```

## 🔧 Configuração

### Banco de Dados
- **Host**: 10.70.11.61
- **Porta**: 1433
- **Database**: siscor_seconserva
- **Tipo**: SQL Server
- **Credenciais**: Infisical (`/db-siscor`)

### BigQuery
- **Dataset**: `infraestrutura_siscor_obras`
- **Projeto**: `rj-iplanrio`

## 📊 Tabelas

### processo_autorizacao_obra
Processos de autorização de obras públicas e privadas com informações completas sobre requerentes, executores, pareceres e localização.

**Schedule**: Diário às 01:00 (America/Sao_Paulo)

**Colunas principais**:
- Secretaria
- Processo (número formatado)
- sqnc_processo
- dscr_tp_processo (tipo de processo)
- Datas: protocolo, início obra, fim obra, aprovação, plenário
- Informações da obra: área ocupação, prazo execução, observação
- Classificações: natureza_obra, tp_obra, lcl_obra
- Requerente: Cgc_Requerente, requerente (nome)
- Executor: cgc_executor, executor (nome fantasia)
- Localização: cdg_logradouro, nm_lgrdr
- Status: dscr_situacao, dscr_parecer
- Licença: nmr_licenca

## 🚀 Uso

### Executar manualmente

```python
from pipelines.rj_seconserva__infraestrutura_siscor_obras.flow import (
    rj_seconserva__infraestrutura_siscor_obras
)

# Executar para a tabela de processos
rj_seconserva__infraestrutura_siscor_obras(
    table_id="processo_autorizacao_obra"
)
```

### Deploy via Prefect CLI

```bash
# Build e deploy
prefect deploy -n rj-seconserva--infraestrutura-siscor-obras--prod

# Deploy apenas staging (sem schedule)
prefect deploy -n rj-seconserva--infraestrutura-siscor-obras--staging
```

## 🔄 Migração Prefect 1.4 → 3.0

### Principais mudanças

1. **Flow unificado**: Antes havia um flow por tabela, agora um único flow processa todas as tabelas via parâmetro `table_id`

2. **Schedules**: Migrados de `Schedule` + `Clock` para formato YAML nativo do Prefect 3.0

3. **Secrets**: Migrado de Vault (`vault_secret_path`) para Infisical (`infisical_secret_path`)

4. **Tasks**: Atualizadas para nova API do Prefect 3.0 com decoradores `@flow` e `@task`

5. **Configuração**: Centralizada em `constants.py` com dataclasses tipadas

### Arquivo original
- **Repositório**: https://github.com/prefeitura-rio/pipelines
- **Path**: `pipelines/rj_seconserva/dump_db_siscor/`

## 📝 Adicionando novas tabelas

Para adicionar uma nova tabela ao pipeline:

1. Adicione a configuração em `constants.py`:

```python
TABLE_CONFIGS = {
    # ... tabelas existentes ...
    "nome_da_nova_tabela": TableConfig(
        table_id="nome_da_nova_tabela",
        execute_query="""
            SELECT * FROM sua_tabela
        """,
        dump_mode="overwrite",
        materialize_after_dump=True,
        materialization_mode="prod",
    ),
}
```

2. Adicione o schedule em `prefect.yaml`:

```yaml
- interval: 86400
  anchor_date: "2022-12-19T01:10:00"
  timezone: America/Sao_Paulo
  slug: nome-da-nova-tabela-daily
  parameters:
    table_id: nome_da_nova_tabela
```

## 🏗️ Desenvolvimento

### Requisitos
- Python 3.11+
- Prefect 3.4.3+
- Acesso ao banco SISCOR
- Credenciais BigQuery configuradas

### Testes locais

```bash
# Instalar dependências
uv sync --package rj_seconserva__infraestrutura_siscor_obras

# Executar flow
uv run python -c "
from pipelines.rj_seconserva__infraestrutura_siscor_obras.flow import rj_seconserva__infraestrutura_siscor_obras
rj_seconserva__infraestrutura_siscor_obras(table_id='processo_autorizacao_obra')
"
```

## 📚 Referências

- [Documentação Prefect 3.0](https://docs.prefect.io/)
- [Template de migração](.claude/skills/migration-pipelines-prefect-v3/SKILL.md)
- [Repositório pipelines original](https://github.com/prefeitura-rio/pipelines)
