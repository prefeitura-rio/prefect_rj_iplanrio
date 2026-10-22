-- Eventos de mensagem WhatsApp (DLL, não DMO — não é do Agentforce).
--
-- Dois filtros de data, de propósito:
--   - cdp_sys_PartitionDate__c: partição da DLL (meia-noite UTC, 1 por dia) —
--     é o que faz pruning. {{ partition_start }}/{{ partition_end }} são inclusivos.
--   - EventDateTime__c: recorta a janela exata dentro da(s) partição(ões).
--     Sem ele, cada tick de 15min puxaria o dia UTC inteiro.
-- Checado em 2026-09-29 no histórico todo (3,1M linhas): EventDateTime__c
-- sempre cai dentro do dia da partição, nunca fora.
--
-- Diferente das DMOs (Agentforce, conversation_entry, tracing), esta DLL
-- grava UTC de verdade (não hora-parede de SP com rótulo Z) —
-- {{ start }}/{{ end }} chegam aqui já convertidos pra UTC
-- (janela_utc em tabelas.yaml e em utils/janela.py).
SELECT
    EventId__c,
    EventDateTime__c,
    EventType__c,
    SendStatus__c,
    NotSentReason__c,
    ErrorCode__c,
    ContactPointPhoneNumber__c,
    IndividualID__c,
    KQ_IndividualID__c,
    WhatsAppWamId__c,
    MessageID__c,
    BulkMessageId__c,
    ContentId__c,
    Message__c,
    MessagePurpose__c,
    PricingCategory__c,
    JourneyId__c,
    FlowElementRunID__c,
    KQ_FlowElementRunID__c,
    KQ_EventId__c,
    EngagementChannel__c,
    KQ_EngagementChannel__c,
    Application__c,
    Source__c,
    SenderId__c,
    SenderDisplayName__c,
    Username__c,
    BusinessUnitID__c,
    ExternalAccountId__c,
    Tenant__c,
    BSUID__c,
    ParentBSUID__c,
    CTWACampaignSourceType__c,
    CTWACampaignSourceId__c,
    CTWAClickId__c,
    LinkURL__c,
    ResolvedURL__c,
    UserAgent__c,
    GlobalEvent__c,
    DataSource__c,
    DataSourceObject__c,
    cdp_sys_SourceVersion__c,
    cdp_sys_PartitionDate__c
FROM MessagingEventsWhatsAppV2_00Das_4CAB1BC2__dll
WHERE cdp_sys_PartitionDate__c >= '{{ partition_start }}'
  AND cdp_sys_PartitionDate__c <= '{{ partition_end }}'
  AND EventDateTime__c >= '{{ start }}'
  AND EventDateTime__c <  '{{ end }}'
