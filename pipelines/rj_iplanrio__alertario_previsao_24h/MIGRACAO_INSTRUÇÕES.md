# Instruções para Migração - Remover Coluna `teve_chuva`

## O que vai acontecer?

Após o deploy, na **primeira execução** do flow, o sistema irá:

1. Detectar que as 4 tabelas têm a coluna `teve_chuva`
2. Criar versões `_v2` de cada tabela **SEM** a coluna `teve_chuva`
3. Avisar nos logs que as tabelas `_v2` foram criadas
4. **Continuar executando normalmente** (dados novos vão para a tabela antiga)

## ⚠️ Ação Necessária - Você Deve Executar

### Passo 1: Verificar as Tabelas _v2

Após a primeira execução do flow, verifique no BigQuery console:
- `rj-iplanrio.brutos_alertario.previsao_diaria_v2`
- `rj-iplanrio.brutos_alertario.dim_previsao_periodo_v2`
- `rj-iplanrio.brutos_alertario.dim_temperatura_zona_v2`
- `rj-iplanrio.brutos_alertario.dim_mares_v2`

**Confira**:
- Número de registros está correto?
- Colunas estão corretas (sem `teve_chuva`, com `hora_execucao`)?
- Dados parecem ok?

### Passo 2: Executar Comandos SQL

**Quando estiver satisfeito** com as tabelas `_v2`, execute os comandos abaixo no BigQuery:

```sql
-- ============================================================================
-- Tabela 1: previsao_diaria
-- ============================================================================
DROP TABLE `rj-iplanrio.brutos_alertario.previsao_diaria`;

CREATE OR REPLACE TABLE `rj-iplanrio.brutos_alertario.previsao_diaria`
AS SELECT * FROM `rj-iplanrio.brutos_alertario.previsao_diaria_v2`;

DROP TABLE `rj-iplanrio.brutos_alertario.previsao_diaria_v2`;


-- ============================================================================
-- Tabela 2: dim_previsao_periodo
-- ============================================================================
DROP TABLE `rj-iplanrio.brutos_alertario.dim_previsao_periodo`;

CREATE OR REPLACE TABLE `rj-iplanrio.brutos_alertario.dim_previsao_periodo`
AS SELECT * FROM `rj-iplanrio.brutos_alertario.dim_previsao_periodo_v2`;

DROP TABLE `rj-iplanrio.brutos_alertario.dim_previsao_periodo_v2`;


-- ============================================================================
-- Tabela 3: dim_temperatura_zona
-- ============================================================================
DROP TABLE `rj-iplanrio.brutos_alertario.dim_temperatura_zona`;

CREATE OR REPLACE TABLE `rj-iplanrio.brutos_alertario.dim_temperatura_zona`
AS SELECT * FROM `rj-iplanrio.brutos_alertario.dim_temperatura_zona_v2`;

DROP TABLE `rj-iplanrio.brutos_alertario.dim_temperatura_zona_v2`;


-- ============================================================================
-- Tabela 4: dim_mares
-- ============================================================================
DROP TABLE `rj-iplanrio.brutos_alertario.dim_mares`;

CREATE OR REPLACE TABLE `rj-iplanrio.brutos_alertario.dim_mares`
AS SELECT * FROM `rj-iplanrio.brutos_alertario.dim_mares_v2`;

DROP TABLE `rj-iplanrio.brutos_alertario.dim_mares_v2`;
```

### Passo 3: Confirmar Migração

Após executar os comandos:

1. Verifique que as tabelas originais agora **não têm** `teve_chuva`
2. Verifique que as tabelas têm a coluna `hora_execucao`
3. Próxima execução do flow irá:
   - Auto-detect: "✅ Nenhuma migração necessária"
   - Inserir dados novos normalmente

## Schema Novo vs Antigo

### ANTES (com teve_chuva):
```
previsao_diaria:
  - id_previsao
  - create_date
  - data_referencia
  - sinotico
  - temp_min_geral
  - temp_max_geral
  - teve_chuva        ← REMOVIDA
  - data_particao
```

### DEPOIS (com hora_execucao):
```
previsao_diaria:
  - id_previsao
  - create_date
  - hora_execucao     ← NOVA
  - data_referencia
  - sinotico
  - temp_min_geral
  - temp_max_geral
  - data_particao
```

## FAQ

### O que fazer se algo der errado?

Se houver problema com as tabelas `_v2`:
1. **NÃO delete** as tabelas originais ainda
2. As tabelas originais continuam intactas
3. Você pode simplesmente deletar as `_v2` e tentar novamente

### Preciso parar o flow durante a migração?

Não é obrigatório, mas recomendado para evitar:
- Dados novos indo para tabela antiga durante a transição
- Confusão sobre qual tabela está ativa

### Quanto tempo demora?

- Criação das tabelas `_v2`: Alguns segundos a minutos (dependendo do tamanho)
- Execução manual dos comandos: Alguns segundos por tabela
- Total: < 10 minutos para dataset pequeno/médio

### Posso executar os comandos um por um?

Sim! Pode fazer tabela por tabela se preferir. Exemplo:
1. Migrar apenas `previsao_diaria` primeiro
2. Verificar que funcionou
3. Migrar as outras 3

## Logs Esperados

### Na primeira execução (antes da migração manual):
```
🔍 Verificando necessidade de migração de tabelas...
🔄 Tabela previsao_diaria: teve_chuva encontrada. Criando previsao_diaria_v2...
Executando: CREATE TABLE previsao_diaria_v2...
✅ Tabela previsao_diaria_v2 criada com 1,234 registros
⚠️  AÇÃO NECESSÁRIA:
    1. Verificar dados em previsao_diaria_v2
    2. Quando estiver ok, executar manualmente:
       DROP TABLE `rj-iplanrio.brutos_alertario.previsao_diaria`;
       CREATE OR REPLACE TABLE `rj-iplanrio.brutos_alertario.previsao_diaria` AS SELECT * FROM `rj-iplanrio.brutos_alertario.previsao_diaria_v2`;
       DROP TABLE `rj-iplanrio.brutos_alertario.previsao_diaria_v2`;
⚠️  4 tabela(s) migraram para _v2. Verifique os logs!
```

### Após a migração manual:
```
🔍 Verificando necessidade de migração de tabelas...
✅ Tabela previsao_diaria: não tem teve_chuva. Migração não necessária.
✅ Tabela dim_previsao_periodo: não tem teve_chuva. Migração não necessária.
✅ Tabela dim_temperatura_zona: não tem teve_chuva. Migração não necessária.
✅ Tabela dim_mares: não tem teve_chuva. Migração não necessária.
✅ Nenhuma migração necessária
```

---

**Data**: 2025-11-26
**Pipeline**: `rj_iplanrio__alertario_previsao_24h`
**Dataset**: `rj-iplanrio.brutos_alertario`
