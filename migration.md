# Plano de Migração - Prefect v1.0 → v3.0

**Total de Pipelines**: 51 pipelines a migrar
**Última atualização**: 2026-03-25

---

## 📋 Índice por Complexidade

- [Pipelines Simples (Dump DB / Templates)](#pipelines-simples-dump-db--templates) - 15 pipelines
- [Pipelines de Complexidade Moderada](#pipelines-de-complexidade-moderada) - 19 pipelines
- [Pipelines Complexas](#pipelines-complexas) - 17 pipelines

---

## Pipelines Simples (Dump DB / Templates)

Pipelines que seguem padrão de dump de banco de dados ou podem usar templates existentes.

### IPLANRIO

#### taxirio
- **Status Atual**: 🟠 Parcial (modelos migrados, pipeline não)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_iplanrio/tree/main/pipelines/taxirio)
- **DBT v1**: -
- **DBT v2**: [link](https://github.com/prefeitura-rio/queries-rj-iplanrio/tree/master/models/raw/iplanrio/taxirio)
- **Checklist**:
  - [ ] Pipeline migrada
  - [x] DBT migrado

#### sici
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_iplanrio/tree/main/pipelines/sici)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### dump_equipamentos
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_iplanrio/tree/main/pipelines/dump_equipamentos)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### processorio
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_iplanrio/tree/main/pipelines/processorio)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### SMFP

#### ergon_comlurb
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_smfp/tree/main/pipelines/ergon_comlurb)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### fincon
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_smfp/tree/main/pipelines/fincon)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### iptu_inad
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_smfp/tree/main/pipelines/iptu_inad)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### atividade_economica
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_smfp/tree/main/pipelines/atividade_economica)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### SECONSERVA

#### siscor
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_seconserva/tree/main/pipelines/siscor)
- **DBT v1**: -
- **DBT v2**: -
- **Observação**: Existe infraestrutura_siscor_obras com modelos v1
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### infraestrutura_siscor_obras
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-seconserva/tree/master/models/infraestrutura_siscor_obras)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### SMI

#### infraestrutura_siscob_obras
- **Status Atual**: 🟠 Parcial (modelos v1 e v2)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-smi/tree/master/models/infraestrutura_siscob_obras)
- **DBT v2**: [link](https://github.com/prefeitura-rio/queries-rj-iplanrio/tree/master/models/raw/smi/infraestrutura_siscob_obras)
- **Observação**: Existe pipeline siscob migrada, verificar diferenças
- **Checklist**:
  - [ ] Pipeline migrada
  - [x] DBT migrado (parcial)

### RIOAGUAS

#### saneamento_drenagem
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_rioaguas/tree/main/pipelines/saneamento_drenagem)
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-rioaguas/tree/master/models/saneamento_drenagem)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### SMDUE

#### sislic
- **Status Atual**: 🟠 Parcial (modelos migrados, pipeline não)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_smdue/tree/main/pipelines/sislic)
- **DBT v1**: -
- **DBT v2**: [link](https://github.com/prefeitura-rio/queries-rj-iplanrio/tree/master/models/raw/smdue/licencas)
- **Checklist**:
  - [ ] Pipeline migrada
  - [x] DBT migrado

### CETRIO

#### ocr_radar
- **Status Atual**: 🟠 Parcial (modelos migrados, pipeline não)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_cetrio/tree/main/pipelines/ocr_radar)
- **DBT v1**: -
- **DBT v2**: [link](https://github.com/prefeitura-rio/queries-rj-iplanrio/tree/master/models/raw/cetrio/ocr_radar)
- **Checklist**:
  - [ ] Pipeline migrada
  - [x] DBT migrado

---

## Pipelines de Complexidade Moderada

Pipelines com lógica de transformação moderada e processamento específico.

### SME

#### educacao_basica
- **Status Atual**: 🟠 Parcial (v3 incompleto)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_sme/tree/main/pipelines/educacao_basica)
- **Prefect v3**: [link](https://github.com/prefeitura-rio/prefect_rj_iplanrio/tree/master/pipelines/rj_sme__educacao_basica)
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-sme/tree/master/models/educacao_basica)
- **DBT v2**: [link](https://github.com/prefeitura-rio/queries-rj-iplanrio/tree/master/models/raw/sme/educacao_basica)
- **Observação**: Faltam frequencia e avaliacao
- **Checklist**:
  - [ ] Pipeline migrada (completar)
  - [x] DBT migrado (parcial)

#### educacao_basica_frequencia
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_sme/tree/main/pipelines/educacao_basica_frequencia)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### educacao_basica_avaliacao
- **Status Atual**: 🟠 Parcial (modelos migrados, pipeline não)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_sme/tree/main/pipelines/educacao_basica_avaliacao)
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-sme/tree/master/models/educacao_basica_avaliacao)
- **DBT v2**: [link](https://github.com/prefeitura-rio/queries-rj-iplanrio/tree/master/models/raw/sme/educacao_basica_avaliacao)
- **Checklist**:
  - [ ] Pipeline migrada
  - [x] DBT migrado

#### educacao_basica_alocacao
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-sme/tree/master/models/educacao_basica_alocacao)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### SEGOVI

#### adm_central_atendimento_1746
- **Status Atual**: 🟠 Parcial (apenas dump_db v3)
- **Prefect v1**: -
- **Prefect v3**: [link](https://github.com/prefeitura-rio/prefect_rj_iplanrio/tree/master/pipelines/rj_segovi__dump_db_1746)
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-segovi/tree/master/models/adm_central_atendimento_1746)
- **DBT v2**: [link](https://github.com/prefeitura-rio/queries-rj-iplanrio/tree/master/models/raw/segovi/adm_central_atendimento_1746)
- **Observação**: Verificar se dump_db cobre todos os casos
- **Checklist**:
  - [ ] Pipeline migrada (completar)
  - [x] DBT migrado (parcial)

#### administracao_servicos_publicos
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-segovi/tree/master/models/administracao_servicos_publicos)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### adm_processorio_sicop
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-segovi/tree/master/models/adm_processorio_sicop)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### SMFP

#### egpweb_metas
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_smfp/tree/main/pipelines/egpweb_metas)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### receita_federal_cnpj
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_smfp/tree/main/pipelines/receita_federal_cnpj)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### SMI

#### infraestrutura_siscob_obras_dashboard
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-smi/tree/master/models/infraestrutura_siscob_obras_dashboard)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### PGM

#### adm_financas_divida_ativa
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-pgm/tree/master/models/adm_financas_divida_ativa)
- **DBT v2**: -
- **Observação**: Existe divida_ativa migrada, verificar se é a mesma
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### CETRIO

#### transporte_rodoviario_radar_transito
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-cetrio/tree/master/models/transporte_rodoviario_radar_transito)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### SMAC

#### povo_comunidades_tradicionais
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_smac/tree/main/pipelines/povo_comunidades_tradicionais)
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-smac/tree/master/models/povo_comunidades_tradicionais)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### povo_comunidades_tradicionais_indepit
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-smac/tree/master/models/povo_comunidades_tradicionais_indepit)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### ESCRITORIO

#### seconserva_buracos
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-escritorio/tree/master/models/seconserva_buracos)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### bases_avulsas
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-escritorio/tree/master/models/bases_avulsas)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### cetrio
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-escritorio/tree/master/models/cetrio)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

---

## Pipelines Complexas

Pipelines com lógica específica, integrações complexas ou processamento avançado.

### COR

#### meteorologia
- **Status Atual**: 🟠 Parcial (modelos parcialmente migrados)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_cor/tree/main/pipelines/meteorologia)
- **DBT v1**: -
- **DBT v2**: [link](https://github.com/prefeitura-rio/queries-rj-iplanrio/tree/master/models/raw/cor/clima_pluviometro)
- **Observação**: Modelos de clima_pluviometro via Airbyte
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado (parcial)

#### rj_escritorio
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_cor/tree/main/pipelines/rj_escritorio)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### teste_pipeline
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_cor/tree/main/pipelines/teste_pipeline)
- **DBT v1**: -
- **DBT v2**: -
- **Observação**: Verificar se ainda é necessário
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### adm_cor_comando
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-cor/tree/master/models/adm_cor_comando)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### meio_ambiente_clima
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-cor/tree/master/models/meio_ambiente_clima)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### ESCRITORIO

#### chatbot
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_escritorio/tree/main/pipelines/chatbot)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### healthcheck
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_escritorio/tree/main/pipelines/healthcheck)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### identidade_unica
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_escritorio/tree/main/pipelines/identidade_unica)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### lgpd
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_escritorio/tree/main/pipelines/lgpd)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### mapa_realizacoes
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_escritorio/tree/main/pipelines/mapa_realizacoes)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### stress
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_escritorio/tree/main/pipelines/stress)
- **DBT v1**: -
- **DBT v2**: -
- **Observação**: Verificar se ainda é necessário
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### SEOP

#### conservacao_ambiental
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_seop/tree/main/pipelines/conservacao_ambiental)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### google_earth_engine
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_seop/tree/main/pipelines/google_earth_engine)
- **DBT v1**: -
- **DBT v2**: -
- **Observação**: Integração com Google Earth Engine
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### conservacao_ambiental_monitor_verde
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-seop/tree/master/models/conservacao_ambiental_monitor_verde)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### dashboard_construcoes_irregulares
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-seop/tree/master/models/dashboard_construcoes_irregulares)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### urbanismo_geosislic_licenciamento
- **Status Atual**: 🔴 v1 (apenas modelos)
- **Prefect v1**: -
- **DBT v1**: [link](https://github.com/prefeitura-rio/queries-rj-seop/tree/master/models/urbanismo_geosislic_licenciamento)
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### IPLANRIO

#### dbt_transform
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_iplanrio/tree/main/pipelines/dbt_transform)
- **DBT v1**: -
- **DBT v2**: -
- **Observação**: Pipeline de transformação DBT, verificar se run_dbt v3 substitui
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

#### painel_obras
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_iplanrio/tree/main/pipelines/painel_obras)
- **DBT v1**: -
- **DBT v2**: -
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

### SMFP

#### dbt_transform
- **Status Atual**: 🔴 v1 (não migrado)
- **Prefect v1**: [link](https://github.com/prefeitura-rio/pipelines_rj_smfp/tree/main/pipelines/dbt_transform)
- **DBT v1**: -
- **DBT v2**: -
- **Observação**: Pipeline de transformação DBT, verificar se run_dbt v3 substitui
- **Checklist**:
  - [ ] Pipeline migrada
  - [ ] DBT migrado

---

## 📊 Resumo de Progresso

### Por Complexidade
- **Simples**: 0/15 (0%)
- **Moderada**: 0/19 (0%)
- **Complexa**: 0/17 (0%)

### Por Secretaria
- **IPLANRIO**: 0/7 (0%)
- **SMFP**: 0/8 (0%)
- **SME**: 0/4 (0%)
- **ESCRITORIO**: 0/9 (0%)
- **COR**: 0/5 (0%)
- **SEOP**: 0/5 (0%)
- **SEGOVI**: 0/3 (0%)
- **CETRIO**: 0/2 (0%)
- **SMAC**: 0/2 (0%)
- **SMI**: 0/2 (0%)
- **SECONSERVA**: 0/2 (0%)
- **RIOAGUAS**: 0/1 (0%)
- **SMDUE**: 0/1 (0%)
- **PGM**: 0/1 (0%)

### Status DBT
- **DBT já migrado**: 7 pipelines
- **DBT parcialmente migrado**: 5 pipelines
- **DBT não migrado**: 39 pipelines

---

## 📝 Observações Gerais

1. **Priorização Sugerida**: Começar pelas pipelines simples (dump_db) para criar momentum
2. **Verificações Necessárias**:
   - Confirmar se `dbt_transform` v1 é substituído por `run_dbt` v3
   - Validar se `divida_ativa` migrada cobre `adm_financas_divida_ativa`
   - Verificar pipelines de teste (`teste_pipeline`, `stress`) se ainda são necessárias
3. **Migrações Parciais**: Completar educacao_basica, adm_central_atendimento_1746, e infraestrutura_siscob_obras
4. **Airbyte**: Verificar quais pipelines podem ser substituídas por ingestão Airbyte
