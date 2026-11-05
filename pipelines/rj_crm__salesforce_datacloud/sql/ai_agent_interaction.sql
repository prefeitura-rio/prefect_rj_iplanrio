-- Agentforce (STDM). Colunas verificadas via SELECT * LIMIT 1 em 2026-07-23.
-- Janela {data_inicio}/{data_fim}: hora-parede de SP com rótulo Z (ver utils/janela.py).
SELECT
    ssot__Id__c,
    ssot__AiAgentSessionId__c,
    ssot__AiAgentInteractionType__c,
    ssot__TopicApiName__c,
    ssot__SessionOwnerId__c,
    ssot__SessionOwnerObject__c,
    ssot__IndividualId__c,
    ssot__TelemetryTraceId__c,
    ssot__TelemetryTraceSpanId__c,
    ssot__PrevInteractionId__c,
    ssot__StartTimestamp__c,
    ssot__EndTimestamp__c,
    ssot__InternalOrganizationId__c,
    ssot__DataSourceId__c,
    ssot__DataSourceObjectId__c,
    ssot__ExternalSourceId__c,
    ssot__AttributeText__c
FROM ssot__AiAgentInteraction__dlm
WHERE ssot__StartTimestamp__c >= '{data_inicio}'
  AND ssot__StartTimestamp__c <  '{data_fim}'
