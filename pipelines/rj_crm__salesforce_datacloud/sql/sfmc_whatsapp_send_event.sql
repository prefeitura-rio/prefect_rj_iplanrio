-- Eventos de envio WhatsApp do Marketing Cloud (DLL do SFMC, MID 534019838).
-- Traz o SubscriberKey (= CPF) por MessageKey__c, que casa com
-- MessageID__c de messaging_events_whatsapp.
--
-- Dois filtros de data, de propósito (mesmo padrão de messaging_events_whatsapp.sql):
--   - cdp_sys_PartitionDate__c: partição da DLL (meia-noite UTC, 1 por dia) —
--     é o que faz pruning. {particao_inicio}/{particao_fim} são inclusivos.
--   - EngagementDateTime__c: recorta a janela exata dentro da(s) partição(ões).
-- Checado em 2026-10-02 no histórico todo (36k linhas, desde 2026-07-09):
-- EngagementDateTime__c nunca nulo e sempre dentro do dia da partição;
-- ID__c único e nunca vazio. AcceptedTimeUTC__c NÃO serve (77 linhas fora
-- da partição).
--
-- Grava UTC de verdade (EngagementDateTime__c bate com EventDateTime__c da
-- DLL de eventos pro mesmo MessageID) — {data_inicio}/{data_fim} chegam aqui
-- já convertidos pra UTC (janela_utc em tabelas.yaml e em utils/janela.py).
SELECT
    ID__c,
    EngagementDateTime__c,
    AcceptedTimeUTC__c,
    LastModifiedDate__c,
    EngagementChannelAction__c,
    MessageRecipientSendStatus__c,
    EventDirection__c,
    Reason__c,
    EngagementActionReason__c,
    EngagementNotesTxt__c,
    MessageKey__c,
    BulkMessageId__c,
    KQ_BulkMessageId__c,
    JourneyID__c,
    JourneyActivityID__c,
    AssetID__c,
    MessageText__c,
    MobileNumber__c,
    CountryCode__c,
    ChannelID__c,
    WhatsAppId__c,
    BSUID__c,
    SubscriberKey__c,
    KQ_SubscriberKey__c,
    SubscriberID__c,
    ContactPointId__c,
    KQ_ContactPointId__c,
    EngagementChannelType__c,
    KQ_EngagementChannelType__c,
    OmniPostModelTypeID__c,
    AppID__c,
    BusinessManagerID__c,
    InternalOrganization__c,
    SenderDisplayName__c,
    DataSource__c,
    DataSourceObject__c,
    KQ_ID__c,
    cdp_sys_SourceVersion__c,
    cdp_sys_PartitionDate__c
FROM sfmc_whatsapp_send_event_534019838__dll
WHERE cdp_sys_PartitionDate__c >= '{particao_inicio}'
  AND cdp_sys_PartitionDate__c <= '{particao_fim}'
  AND EngagementDateTime__c >= '{data_inicio}'
  AND EngagementDateTime__c <  '{data_fim}'
