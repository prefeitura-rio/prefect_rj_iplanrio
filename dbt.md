# Diagnóstico: DBT nas Pipelines do Prefect

## Resumo Executivo

O repositório utiliza o DBT (Data Build Tool) de duas formas principais nas pipelines do Prefect:

1. **Pipeline dedicada de execução automática** (`rj_iplanrio__run_dbt`)
2. **Materialização sob demanda** através da função `execute_dbt_task` em flows específicos

## 1. Pipeline Dedicada: `rj_iplanrio__run_dbt`

### Localização
- **Flow principal**: `pipelines/rj_iplanrio__run_dbt/flow.py`
- **Utilitários**: `pipelines/rj_iplanrio__run_dbt/utils.py`
- **Configuração**: `pipelines/rj_iplanrio__run_dbt/prefect.yaml`

### Funcionalidades Principais

#### 1.1 Execução de Comandos DBT
- Suporte completo aos comandos DBT: `run`, `test`, `build`, `source freshness`, `deps`, `seed`, `snapshot`
- Utiliza `PrefectDbtRunner` para execução
- Permite seleção de modelos através de tags, includes e excludes
- Suporte a flags adicionais

#### 1.2 Gerenciamento de Artefatos
- **Download**: Baixa artefatos do DBT do Google Cloud Storage para execução incremental
- **Upload**: Envia artefatos gerados de volta para o GCS (apenas em produção)
- **Localização**: Buckets configuráveis por ambiente (`prod`: `rj-iplanrio_dbt`, `dev`: `rj-iplanrio-dev_dbt`)

#### 1.3 Gestão de Repositório
- Clona automaticamente o repositório de queries do GitHub: `https://github.com/prefeitura-rio/queries-rj-iplanrio.git`
- Instala dependências DBT através do comando `deps`
- Limpa e recria o diretório de trabalho a cada execução

#### 1.4 Relatórios e Notificações
- Geração automática de relatórios de execução
- Envio de notificações para Discord com:
  - Status de sucesso/falha
  - Detalhes dos modelos que falharam
  - Parâmetros de execução
  - Logs detalhados
- Processamento e análise de logs do DBT

### 1.2 Scheduling Automático

#### Schedules Configurados (Produção)

**1. Verificação Diária de Freshness**
- **Horário**: 09:00 (América/São_Paulo)
- **Comando**: `source freshness`
- **Frequência**: Diária
- **Objetivo**: Verificar se as fontes de dados estão atualizadas

**2. Build Diário**
- **Horário**: 09:15 (América/São_Paulo)
- **Comando**: `build`
- **Seleção**: `tag:daily`
- **Frequência**: Diária
- **Objetivo**: Materializar modelos marcados com tag "daily"

**3. Build Semanal**
- **Horário**: 09:15 às segundas-feiras (América/São_Paulo)
- **Comando**: `build`
- **Seleção**: `tag:weekly`
- **Frequência**: Semanal
- **Objetivo**: Materializar modelos marcados com tag "weekly"

### 1.3 Parâmetros Configuráveis
- `command`: Comando DBT a executar
- `target`: Ambiente (dev/prod)
- `select`: Seleção de modelos (tags, nomes, etc.)
- `exclude`: Exclusão de modelos
- `flag`: Flags adicionais
- `github_repo`: Repositório de queries
- `bigquery_project`: Projeto BigQuery de destino
- `gcs_buckets`: Configuração de buckets por ambiente
- `send_discord_report`: Controle de notificações

## 2. Materialização Sob Demanda

### Implementação
Utiliza a função `execute_dbt_task` da biblioteca `iplanrio.pipelines_utils.dbt` para execução pontual de modelos DBT ao final de flows de ingestão.

### Casos de Uso Identificados

#### 2.1 Pipeline `rj_iplanrio__eai_history`
```python
execute_dbt_task(select=dbt_select, target="prod")
```
- **Localização**: `pipelines/rj_iplanrio__eai_history/flow.py:52`
- **Seleção padrão**: `--select raw_eai_logs_history`
- **Momento**: Após upload de dados históricos do EAI
- **Objetivo**: Materializar modelo específico após ingestão

#### 2.2 Pipeline `rj_smas__api_datametrica_agendamentos`
```python
execute_dbt_task(select=dbt_select, target="prod")
```
- **Localização**: `pipelines/rj_smas__api_datametrica_agendamentos/flow.py:122`
- **Seleção**: `raw_cadunico_agendamentos`
- **Condicional**: Apenas quando `materialize_after_dump=True`
- **Momento**: Após upload de agendamentos da API Datametrica
- **Objetivo**: Transformar dados brutos recém-ingeridos

### 2.3 Referências em Outros Flows
Identificadas referências comentadas em:
- `rj_smas__api_datametrica_agendamentos/flow.py:7`
- `rj_smas__disparo_cadunico/flow.py:7`

Indicando que existia uma funcionalidade `task_run_dbt_model_task` na biblioteca anterior que foi migrada para `execute_dbt_task`.

## 3. Arquitetura e Integração

### 3.1 Biblioteca `iplanrio.pipelines_utils`
- **DBT Module**: `iplanrio.pipelines_utils.dbt`
- **Função principal**: `execute_dbt_task`
- **Funcionalidade**: Execução simplificada de comandos DBT específicos

### 3.2 Infraestrutura
- **Executor**: Prefect 3.0 com workpools Kubernetes
- **Imagens Docker**: Construídas automaticamente via GitHub Actions
- **Credenciais**: Injeção automática via `inject_bd_credentials_task`
- **Armazenamento**: Google Cloud Storage para artefatos DBT

### 3.3 Monitoramento
- **Logs**: Captura e processamento de logs DBT
- **Discord**: Notificações automáticas de status
- **Prefect UI**: Visualização de execuções e logs

## 4. Padrões e Boas Práticas Identificadas

### 4.1 Tagueamento de Modelos
- **Tag `daily`**: Modelos executados diariamente
- **Tag `weekly`**: Modelos executados semanalmente
- **Seleção específica**: Modelos individuais para pipelines de ingestão

### 4.2 Estratégia de Ambientes
- **Produção**: Execução automática com schedules
- **Desenvolvimento**: Execução manual via UI
- **Targets**: Configuração de ambientes DBT separados

### 4.3 Gestão de Estado
- **Artefatos GCS**: Manutenção de estado para execução incremental
- **State comparison**: Utilização do parâmetro `--state` para otimização

## 5. Limitações e Considerações

### 5.1 Dependências
- Pipeline dedicada depende do repositório externo `queries-rj-iplanrio`
- Necessidade de credenciais configuradas corretamente
- Dependência de buckets GCS específicos

### 5.2 Execução
- Pipeline dedicada executa clone completo do repositório a cada run
- Não há cache de dependências DBT entre execuções
- Execução sequencial (não paralela) de comandos

### 5.3 Monitoramento
- Logs Discord limitados a 2000 caracteres
- Sistema de notificação de incidentes comentado (possivelmente desabilitado)

## 6. Recomendações

### 6.1 Otimizações
1. **Cache de dependências**: Implementar cache de `dbt_packages` entre execuções
2. **Paralelização**: Considerar execução paralela de testes e builds quando apropriado
3. **Seletividade**: Expandir uso de tags para melhor controle de execução

### 6.2 Monitoramento
1. **Reativar sistema de tickets**: Considerar reabilitar criação automática de incidentes
2. **Métricas**: Implementar coleta de métricas de performance
3. **Alertas**: Configurar alertas mais granulares por tipo de falha

### 6.3 Governança
1. **Documentação**: Criar documentação específica sobre tagueamento de modelos
2. **Testes**: Expandir cobertura de testes automáticos
3. **Versionamento**: Implementar versionamento de modelos DBT