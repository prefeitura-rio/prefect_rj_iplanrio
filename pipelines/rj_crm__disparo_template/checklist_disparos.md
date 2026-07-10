# Checklist de Disparos — rj_crm__disparo_template

Jornadas ativas em desenvolvimento: Dívida Ativa, Puérperas e CadÚnico. (CVL 1746 e Cartão Primeira Infância ficam de fora por enquanto.)

Para cada etapa, marcar:
- [ ] **Query prod retornou dados** — rodada manualmente contra as tabelas de produção
- [ ] **Disparo testado fim a fim** — schedule no Prefect → mensagem chegando no Salesforce/WhatsApp
- [ ] **Jornada testada fim a fim** — todas as possiveis sequencias de mensagens validadas 

---

## 1. PGM Dívida Ativa

Sequência: cobrança → lembrete → agradecimento (cada uma depende do histórico em `status_disparo` da anterior).

### Cobrança
- campaign_name: `pgm_divida_ativa_prod_v2`
- Prod: `queries/pgm_divida_ativa_cobranca.sql`
- Dev: `queries_dev/pgm_divida_ativa_cobranca.sql` 
- Test: `queries_test/pgm_divida_ativa_cobranca__test.sql`
- Tabelas prod: `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`, `rj-iplanrio.divida_ativa.contribuinte`, `rj-crm-registry.brutos_salesforce.status_disparo`
- Tabelas dev: `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte`, `rj-crm-registry.brutos_salesforce.status_disparo`
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

### Lembrete
- campaign_name: `pgmdividaativalembreteprodv1`
- Prod: `queries/pgm_divida_ativa_lembrete.sql`
- Dev: `queries_dev/pgm_divida_ativa_lembrete.sql` 
- Test: `queries_test/pgm_divida_ativa_lembrete__test.sql`
- Tabelas prod: `rj-iplanrio.divida_ativa.contribuinte`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`
- Tabelas dev: `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte`, `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

### Agradecimento
- campaign_name: `pgmdividaativaconfirmapgtoprodv1`
- Prod: `queries/pgm_divida_ativa_agradecimento.sql`
- Dev: `queries_dev/pgm_divida_ativa_agradecimento.sql` 
- Test: `queries_test/pgm_divida_ativa_agradecimento__test.sql`
- Tabelas prod: `rj-crm-registry.brutos_salesforce.status_disparo`, `rj-iplanrio.divida_ativa.contribuinte`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`
- Tabelas dev: `rj-crm-registry.brutos_salesforce.status_disparo`, `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte`, `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica`
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

---

## 2. SMS Puérperas

Confirmação de agendamento antes do parto + cadeia de pesquisas em diferentes dias pós-alta, todas reaproveitando `sms_puerperas_pesquisa_template.sql` com placeholders diferentes.

### Confirma agendamento pré-parto
- campaign_name: `_sms_puerpera_disp1_gestantev2`
- Prod: `queries/sms_puerpera_confirma_agendamento.sql`
- Dev: `queries_dev/sms_puerpera_confirma_agendamento.sql` 
- Test: `queries_test/sms_puerpera_confirma_agendamento.sql`
- Tabelas prod: `rj-sms.projeto_whatsapp.siscegonha_agendamento_maternidade`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`
- Tabelas dev: `rj-crm-registry-dev.dev__dev_fantasma__brutos_sms.siscegonha_agendamento_maternidade`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

### D0 — Explica pesquisa
- campaign_name: `smspuerperasdisparo25`
- Prod: `queries/sms_puerperas_d0_explica_pesquisa.sql`
- Dev: `queries_dev/sms_puerperas_d0_explica_pesquisa.sql` 
- Test: `queries_test/sms_puerperas_d0_explica_pesquisa.sql`
- Tabelas prod: `rj-sms.projeto_whatsapp.sisare_alta_maternidade`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`
- Tabelas dev: `rj-crm-registry-dev.dev__dev_fantasma__brutos_sms.sisare_alta_maternidade`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

### Pesquisa 1 — D1/D3/D5/D10
- campaign_name: `smspuerperasdisparo4v3`
- Prod: `queries/sms_puerperas_pesquisa_template.sql`
- Dev: `queries_dev/sms_puerperas_pesquisa_template.sql` 
- Test: `queries_test/sms_puerperas_pesquisa_template__pesquisa1.sql`
- Tabelas prod: `rj-sms.projeto_whatsapp.sisare_alta_maternidade`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`
- Tabelas dev: `rj-crm-registry-dev.dev__dev_fantasma__brutos_sms.sisare_alta_maternidade`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

### Pesquisa 4 — D7/D22
- campaign_name: `smspuerperadisparo6`
- Queries: mesmas da Pesquisa 1 (prod/dev)
- Test: `queries_test/sms_puerperas_pesquisa_template__pesquisa4.sql`
- Tabelas prod/dev: idem Pesquisa 1
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

### Pesquisa 5 — D25/D28
- campaign_name: `smspuerperasdisparo12`
- Queries: mesmas da Pesquisa 1 (prod/dev)
- Test: `queries_test/sms_puerperas_pesquisa_template__pesquisa5.sql`
- Tabelas prod/dev: idem Pesquisa 1
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

### Pesquisa 3 — D13/D16/D19 (active: false)
- campaign_name: `smspuerperasdisparo9`
- Queries: mesmas da Pesquisa 1 (prod/dev)
- Test: — (sem arquivo)
- Tabelas prod/dev: idem Pesquisa 1
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

### Pesquisa 6 — D34 (active: false)
- campaign_name: `smspuerperasdisparo14`
- Queries: mesmas da Pesquisa 1 (prod/dev)
- Test: — (sem arquivo)
- Tabelas prod/dev: idem Pesquisa 1
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

### Pesquisa 7 — D40 (active: false)
- campaign_name: `smspuerperasdisparo152`
- Queries: mesmas da Pesquisa 1 (prod/dev)
- Test: `queries_test/sms_puerperas_pesquisa_template__pesquisa7.sql`
- Tabelas prod/dev: idem Pesquisa 1
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

---

## 3. CadÚnico (SMAS)

### Confirma agendamento CRAS
- campaign_name: `confirma_agendamento_cadunico_prod_v2`
- Prod: `queries/cadunico_confirma_agendamento.sql`
- Dev: `queries_dev/cadunico_confirma_agendamento.sql` 
- Test: `queries_test/cadunico_confirma_agendamento__test.sql`
- Tabelas prod: `rj-iplanrio.brutos_data_metrica_staging.cadunico_agendamentos`, `rj-crm-registry.brutos_salesforce.status_disparo`, `rj-crm-registry.intermediario_rmi_telefones.int_telefone`
- Tabelas dev: `rj-crm-registry-dev.dev__dev_fantasma__brutos_data_metrica_staging.cadunico_agendamentos`, `rj-crm-registry.brutos_salesforce.status_disparo`, `rj-crm-registry-dev.dev__dev_fantasma__intermediario_rmi_telefones.int_telefone`
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim
