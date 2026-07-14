# Checklist de Disparos — rj_crm__disparo_template

Jornadas ativas em desenvolvimento: Dívida Ativa, Puérperas, CadÚnico, CVL 1746 e Cartão Primeira Infância (PIC).

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
- Fallback legado (transição, checado em prod/dev/test): `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` (templateId 196/239, via `id_hsm_legado_cobranca_placeholder` no scheduler_sf.yaml)
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim (pendente Fernando)
- [ ] Jornada testada fim a fim (pendente Fernando)

### Lembrete
- campaign_name: `pgmdividaativalembreteprodv1`
- Prod: `queries/pgm_divida_ativa_lembrete.sql`
- Dev: `queries_dev/pgm_divida_ativa_lembrete.sql` 
- Test: `queries_test/pgm_divida_ativa_lembrete__test.sql`
- Tabelas prod: `rj-iplanrio.divida_ativa.contribuinte`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`
- Tabelas dev: `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte`, `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`
- Fallback legado (transição, checado em prod/dev/test): `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` (templateId 196/239 = cobrança, 227 = lembrete, via `id_hsm_legado_cobranca_placeholder`/`id_hsm_legado_lembrete_placeholder` no scheduler_sf.yaml)
- [ ] Query prod retornou dados (provavelmente por conta do novo nome das hsms)
- [x] Disparo testado fim a fim
- [x] Jornada testada fim a fim

### Agradecimento
- campaign_name: `pgmdividaativaconfirmapgtoprodv1`
- Prod: `queries/pgm_divida_ativa_agradecimento.sql`
- Dev: `queries_dev/pgm_divida_ativa_agradecimento.sql` 
- Test: `queries_test/pgm_divida_ativa_agradecimento__test.sql`
- Tabelas prod: `rj-crm-registry.brutos_salesforce.status_disparo`, `rj-iplanrio.divida_ativa.contribuinte`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`
- Tabelas dev: `rj-crm-registry.brutos_salesforce.status_disparo`, `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte`, `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica`
- Fallback legado (transição, checado em prod/dev/test): `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` (templateId 196/239 = cobrança, 227 = lembrete, 231 = agradecimento, via `id_hsm_legado_cobranca_placeholder`/`id_hsm_legado_lembrete_placeholder`/`id_hsm_legado_agradecimento_placeholder` no scheduler_sf.yaml)
- [ ] Query prod retornou dados (provavelmente por conta do novo nome das hsms)
- [ ] Disparo testado fim a fim
- [x] Jornada testada fim a fim

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
- Fallback legado (transição, checado em prod/dev/test): `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` (templateId 573, via `id_hsm_legado_placeholder` no scheduler_sf.yaml)
- [] Query prod retornou dados (não consigo validar sem permissão para leitura da rj-sms)
- [x] Disparo testado fim a fim
- [x] Jornada testada fim a fim

### D0 — Explica pesquisa
- campaign_name: `smspuerperasdisparo25`
- Prod: `queries/sms_puerperas_d0_explica_pesquisa.sql`
- Dev: `queries_dev/sms_puerperas_d0_explica_pesquisa.sql` 
- Test: `queries_test/sms_puerperas_d0_explica_pesquisa.sql`
- Tabelas prod: `rj-sms.projeto_whatsapp.sisare_alta_maternidade`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`
- Tabelas dev: `rj-crm-registry-dev.dev__dev_fantasma__brutos_sms.sisare_alta_maternidade`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`
- Fallback legado (transição, checado em prod/dev/test): `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` (templateId 610, via `id_hsm_legado_placeholder` no scheduler_sf.yaml)
- [ ] Query prod retornou dados (não consigo validar sem permissão para leitura da rj-sms)
- [x] Disparo testado fim a fim
- [x] Jornada testada fim a fim (não tem o lance do input com integração do eai)

### Pesquisa 1 — D1/D3/D5/D10/D13/D16/D19
- Unifica a antiga Pesquisa 3 (D13/D16/D19); schedule `daily-sms-puerperas-d13-d16-d19` foi desativado no scheduler_sf.yaml
- campaign_name: `smspuerperasdisparo4v3`
- Prod: `queries/sms_puerperas_pesquisa_template.sql`
- Dev: `queries_dev/sms_puerperas_pesquisa_template.sql` 
- Test: `queries_test/sms_puerperas_pesquisa_template__pesquisa1.sql`
- Tabelas prod: `rj-sms.projeto_whatsapp.sisare_alta_maternidade`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`
- Tabelas dev: `rj-crm-registry-dev.dev__dev_fantasma__brutos_sms.sisare_alta_maternidade`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`
- Fallback legado (transição, checado em prod/dev/test): `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` (anterior/D0 = 610; desta pesquisa = 588 e 562, via `id_hsm_anterior_legado_placeholder`/`id_hsm_legado_placeholder` no scheduler_sf.yaml)
- [ ] Query prod retornou dados (não consigo validar sem permissão para leitura da rj-sms)
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

### Pesquisa 4 — D7/D22
- campaign_name: `smspuerperadisparo6`
- Queries: mesmas da Pesquisa 1 (prod/dev)
- Test: `queries_test/sms_puerperas_pesquisa_template__pesquisa4.sql`
- Tabelas prod/dev: idem Pesquisa 1
- Fallback legado: idem Pesquisa 1, mas desta pesquisa = 456 (antigo D7) e 489 (antigo D22), já que essa campanha unifica os dois disparos antigos
- [ ] Query prod retornou dados (não consigo validar sem permissão para leitura da rj-sms)
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

### Pesquisa 5 — D25/D28/D34
- Unifica a antiga Pesquisa 6 (D34); schedule `daily-sms-puerperas-d34` foi desativado no scheduler_sf.yaml
- campaign_name: `smspuerperasdisparo12`
- Queries: mesmas da Pesquisa 1 (prod/dev)
- Test: `queries_test/sms_puerperas_pesquisa_template__pesquisa5.sql`
- Tabelas prod/dev: idem Pesquisa 1
- Fallback legado: idem Pesquisa 1, mas desta pesquisa = 564 e 566
- [ ] Query prod retornou dados (não consigo validar sem permissão para leitura da rj-sms)
- [ ] Disparo testado fim a fim (pendente Maiko)
- [ ] Jornada testada fim a fim (pendente Maiko)

### Pesquisa 7 — D40 
- campaign_name: `smspuerperasdisparo152`
- Queries: mesmas da Pesquisa 1 (prod/dev)
- Test: `queries_test/sms_puerperas_pesquisa_template__pesquisa7.sql`
- Tabelas prod/dev: idem Pesquisa 1
- Fallback legado: idem Pesquisa 1, mas desta pesquisa = 589
- [ ] Query prod retornou dados (não consigo validar sem permissão para leitura da rj-sms)
- [ ] Disparo testado fim a fim
- [x] Jornada testada fim a fim

---

## 3. CadÚnico (SMAS)

### Confirma agendamento CRAS
- campaign_name: `confirma_agendamento_cadunico_prod_v2`
- Prod: `queries/cadunico_confirma_agendamento.sql`
- Dev: `queries_dev/cadunico_confirma_agendamento.sql` 
- Test: `queries_test/cadunico_confirma_agendamento__test.sql`
- Tabelas prod: `rj-iplanrio.brutos_data_metrica_staging.cadunico_agendamentos`, `rj-crm-registry.brutos_salesforce.status_disparo`, `rj-crm-registry.intermediario_rmi_telefones.int_telefone`
- Tabelas dev: `rj-crm-registry-dev.dev__dev_fantasma__brutos_data_metrica_staging.cadunico_agendamentos`, `rj-crm-registry.brutos_salesforce.status_disparo`, `rj-crm-registry-dev.dev__dev_fantasma__intermediario_rmi_telefones.int_telefone`
- Fallback legado (transição, checado em prod/dev/test): `rj-crm-registry.crm_whatsapp.telefone_disparado` (id_hsm 101, por telefone)
- [] Query prod retornou dados (vale conferir)
- [x] Disparo testado fim a fim
- [x] Jornada testada fim a fim

---

## 4. CVL 1746 (Central de Atendimento)

Pesquisa de satisfação pós-atendimento de chamados 1746, dois ramos dependendo do desfecho (fechado com solução / sem resolução).

### Pesquisa — Fechado com solução
- campaign_name: `cvl_pesquisa_1746_prod_v1`
- Prod: `queries/cvl_pesquisa_1746_com_solucao.sql`
- Dev: `queries_dev/cvl_pesquisa_1746_com_solucao.sql`
- Test: `queries_test/cvl_pesquisa_1746_com_solucao__test.sql`
- Tabelas prod/dev (mesmas tabelas fonte, sem fantasma dev): `rj-segovi.adm_central_atendimento_1746.chamado`, `chamado_cpf`, `pessoa`, `origem_ocorrencia`, `rj-iplanrio.brutos_extracoes_google_sheets.relacao_bairro_subprefeitura`, `rj-crm-registry.brutos_salesforce.status_disparo`
- Fallback legado (transição, checado em prod/dev/test): `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` (templateId 192, via `id_hsm_placeholder` no scheduler_sf.yaml)
- Schedule `daily-cvl-pesquisa1746-prod` está `active: false` no scheduler_sf.yaml
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

### Pesquisa — Sem resolução
- campaign_name: `cvl_pesquisa_1746_sem_resolucao_prod_v1`
- Prod: `queries/cvl_pesquisa_1746_sem_resolucao.sql`
- Dev: `queries_dev/cvl_pesquisa_1746_sem_resolucao.sql`
- Test: `queries_test/cvl_pesquisa_1746_sem_resolucao__test.sql`
- Tabelas prod/dev: idem Pesquisa — Fechado com solução
- Fallback legado (transição, checado em prod/dev/test): `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` (templateId 291, via `id_hsm_placeholder` no scheduler_sf.yaml)
- Schedule `daily-cvl-1746-survey-invitation-without-resolution` está `active: true` mas com `test_mode: true`
- [ ] Query prod retornou dados
- [ ] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

---

## 5. SMAS Cartão Primeira Infância (PIC)

Aviso de evento de entrega do cartão + lembrete, ambos condicionados a aprovação manual na planilha de bairros de entrega (gate `target_date` na query — sem aprovação do dia, a query nem toca nas tabelas pesadas).

### Aviso de evento
- campaign_name: `smascartaoprimeirainfanciaprodv27`
- Prod: `queries/smas_cartaopic_evento.sql`
- Dev: `queries_dev/smas_cartaopic_evento.sql`
- Test: `queries_test/smas_cartaopic_evento__test.sql`
- Tabelas: `rj-smas-dev.pic.raw_cartao_primeira_infancia_carioca_bairros_entrega`, `rj-smas-dev.pic.cartao_primeira_infancia_carioca_status`, `rj-iplanrio.brutos_data_metrica_staging.cadunico_agendamentos`, `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`, `rj-crm-registry.brutos_salesforce.status_disparo`, `rj-crm-registry.intermediario_rmi_telefones.int_telefone`
- Fallback legado (transição, checado em prod/dev/test): `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` (templateId 184, via `id_hsm_legado_placeholder` no scheduler_sf.yaml)
- [ ] Query prod retornou dados (sem aprovação)
- [x] Disparo testado fim a fim
- [ ] Jornada testada fim a fim

### Lembrete
- campaign_name: `smascartaoprimeirainfancialembreteprodv4`
- Prod: `queries/smas_cartaopic_lembrete.sql`
- Dev: `queries_dev/smas_cartaopic_lembrete.sql`
- Test: `queries_test/smas_cartaopic_lembrete__test.sql`
- Tabelas: idem Aviso de evento
- Fallback legado (transição, checado em prod/dev/test): `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` (templateId 185, via `id_hsm_legado_placeholder` no scheduler_sf.yaml)
- [ ] Query prod retornou dados (sem aprovação)
- [x] Disparo testado fim a fim
- [ ] Jornada testada fim a fim
