# Notas de Migração ERGON - Prefect 1.4 → 3.0

## Resumo da Migração

Pipeline migrada do Prefect 1.4 para Prefect 3.0 para ingestão de dados do ERGON (Sistema de Recursos Humanos) no BigQuery.

### Dataset
- **Dataset ID**: `brutos_ergon`
- **Secretaria**: SMFP (Secretaria Municipal de Fazenda e Planejamento)
- **Banco de Dados**: P01.PCRJ (Oracle)

## Tabelas Migradas

A migração incluiu **14 tabelas** (9 novas + 5 existentes):

### Tabelas Originais (já existentes)
1. `IPL_PT_FICHAS` - Fichas de pagamento detalhadas (append, mensal)
2. `FICHAS_FINANCEIRAS` - Fichas financeiras (append, mensal)
3. `VW_DLK_ERG_FITA_BANCO` - View de fita banco (append, mensal)
4. `TOTAL_CONTA` - Totais de contas (overwrite, semanal)
5. `VANTAGENS` - Vantagens dos funcionários (append, mensal)

### Tabelas Adicionadas (migradas do Ergon Comlurb)
6. `CARGO` - Cargos e suas características (overwrite, diário)
7. `DEPENDENTE` - Dependentes dos funcionários (overwrite, diário)
8. `FUNCIONARIO_EVENTO` - Eventos dos funcionários (overwrite, diário)
9. `FREQUENCIA` - Frequências e ausências (overwrite, diário)
10. `FUNCIONARIO` - Dados cadastrais dos funcionários (overwrite, diário)
11. `SETOR_H` - Histórico de setores (overwrite, diário)
12. `LICENCA_AFASTAMENTO` - Licenças e afastamentos (overwrite, diário)
13. `SETOR` - Setores da organização (overwrite, diário)
14. `VINCULO` - Vínculos dos funcionários (overwrite, diário)

## Principais Mudanças

### 1. Estrutura do Flow
- **Prefect 1.4**: Utilizava `dump_sql_flow` genérico com `deepcopy`
- **Prefect 3.0**: Flow nativo `rj_smfp__dump_db_ergon` com decorador `@flow`

### 2. Schedule
- **Prefect 1.4**: 
  - Intervalo: 1 dia
  - Data inicial: 2022-10-25 23:30
- **Prefect 3.0**: 
  - Intervalo: 24 horas (86400 segundos) para maioria
  - Intervalo: 7 dias (604800 segundos) para TOTAL_CONTA
  - Data inicial: 2025-07-15
  - Escalonamento: 10 minutos entre cada tabela

### 3. Configuração de Deployment
- **Prefect 1.4**: 
  - Storage: GCS
  - Run Config: KubernetesRun
  - Labels: RJ_SMFP_AGENT_LABEL
- **Prefect 3.0**: 
  - Build: Docker image com hash do commit
  - Work Pool: k3s-pool
  - Image: ghcr.io/prefeitura-rio/prefect_rj_iplanrio/deployments

### 4. Credenciais
- **Prefect 1.4**: `vault_secret_path` (não especificado no original)
- **Prefect 3.0**: `infisical_secret_path: "/db-ergon-prod"`

### 5. Parâmetros do Flow
Parâmetros mantidos com alguns ajustes:
- `db_database`: "P01.PCRJ"
- `db_host`: "10.70.6.21"
- `db_port`: "1526"
- `db_type`: "oracle"
- `dataset_id`: "brutos_ergon"
- `only_staging_dataset`: True (mantido)

## Comparação com Ergon Comlurb

### Diferenças de Configuração

| Parâmetro | Ergon SMFP (atual) | Ergon Comlurb (antigo) |
|-----------|-------------------|------------------------|
| Banco | P01.PCRJ | P25 |
| Host | 10.70.6.21 | 10.70.6.26 |
| Porta | 1526 | 1521 |
| Dataset | brutos_ergon | recursos_humanos_ergon_comlurb |
| Secret Path | /db-ergon-prod | /db-ergon-comlurb |

### Tabelas Específicas

Algumas tabelas do SMFP não existem no Comlurb (e vice-versa):

**Apenas no SMFP:**
- `IPL_PT_FICHAS` - View específica da prefeitura
- `VW_DLK_ERG_FITA_BANCO` - View customizada de fita banco

**Apenas no Comlurb (original):**
- `ficha_financeira` - Tabela base (diferente de FICHAS_FINANCEIRAS)
- `fita_banco` - Tabela base (diferente de VW_DLK_ERG_FITA_BANCO)

## Categorização das Tabelas

### Dados Cadastrais (overwrite diário)
- `FUNCIONARIO` - Cadastro completo de funcionários
- `DEPENDENTE` - Dependentes dos funcionários
- `CARGO` - Cargos e funções
- `SETOR` - Estrutura organizacional
- `SETOR_H` - Histórico de setores
- `VINCULO` - Vínculos empregatícios

### Eventos e Movimentações (overwrite/append)
- `FUNCIONARIO_EVENTO` - Eventos funcionais (overwrite)
- `VANTAGENS` - Vantagens e benefícios (append, mensal)
- `FREQUENCIA` - Frequências (overwrite)
- `LICENCA_AFASTAMENTO` - Licenças e afastamentos (overwrite)

### Dados Financeiros (append mensal)
- `IPL_PT_FICHAS` - Fichas de pagamento detalhadas
- `FICHAS_FINANCEIRAS` - Fichas financeiras
- `VW_DLK_ERG_FITA_BANCO` - Fita banco
- `TOTAL_CONTA` - Totais (overwrite semanal)

## Melhorias Implementadas

### Documentação
- Adicionada documentação completa do flow com categorização das tabelas
- Descrição clara do propósito de cada categoria
- Identificação das fontes de dados

### Configuração
- Dataset ID padronizado e explícito em todos os schedules
- Parâmetros consistentes entre todas as tabelas
- Escalonamento otimizado para evitar sobrecarga

### Cobertura de Dados
- 9 novas tabelas adicionadas
- Cobertura completa do modelo de RH (cadastro, eventos, financeiro)
- Alinhamento com estrutura do Ergon Comlurb

## Arquivos Modificados

1. **flow.py**: 
   - Atualizada documentação
   - Mantidos parâmetros padrão
   - Compatibilidade com Prefect 3.0

2. **prefect.yaml**: 
   - Adicionado `dataset_id` em todos os schedules
   - Adicionados 9 novos schedules
   - Configurado deployment para produção
   - Schedules escalonados a cada 10 minutos

## Validação

Para validar a migração:
```bash
# Verificar sintaxe do flow
uv run --package rj_smfp__dump_db_ergon -- prefect flow-run execute

# Validar prefect.yaml
prefect deploy --help

# Listar todas as tabelas
grep "slug:" pipelines/rj_smfp__dump_db_ergon/prefect.yaml
```

## Próximos Passos

1. Testar deployment em ambiente de staging
2. Validar execução de cada tabela
3. Monitorar logs e performance
4. Ajustar batch_size e retry conforme necessário
5. Verificar se há necessidade de adicionar mais tabelas específicas

## Observações Importantes

⚠️ **Atenção**: Este flow acessa o banco **P01.PCRJ** (Ergon da Prefeitura), diferente do **P25** (Ergon Comlurb). As queries foram adaptadas mas podem necessitar ajustes dependendo das diferenças de schema entre os bancos.

⚠️ **Campos FLEX**: Muitas tabelas possuem campos FLEX_CAMPO_XX que podem ter significados diferentes entre os bancos. Validar com a equipe de RH se necessário.

⚠️ **Performance**: Tabelas como `FUNCIONARIO`, `FUNCIONARIO_EVENTO` e `VINCULO` são grandes. Monitorar tempo de execução e considerar otimizações se necessário.
