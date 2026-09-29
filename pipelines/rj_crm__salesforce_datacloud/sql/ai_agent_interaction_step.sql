-- Agentforce (STDM). Colunas verificadas via SELECT * LIMIT 1 em 2026-07-23.
-- Janela {data_inicio}/{data_fim}: hora-parede de SP com rótulo Z (ver utils/janela.py).
SELECT
    ssot__Id__c,
    ssot__AiAgentInteractionId__c,
    ssot__AiAgentInteractionStepType__c,
    ssot__Name__c,
    ssot__TelemetryTraceSpanId__c,
    ssot__GenAiGatewayRequestId__c,
    ssot__GenAiGatewayResponseId__c,
    ssot__GenerationId__c,
    ssot__PrevStepId__c,
    ssot__StartTimestamp__c,
    ssot__EndTimestamp__c,
    ssot__InternalOrganizationId__c,
    ssot__DataSourceId__c,
    ssot__DataSourceObjectId__c,
    ssot__ExternalSourceId__c,
    ssot__PreStepVariableText__c,
    ssot__PostStepVariableText__c,
    ssot__AttributeText__c,
    ssot__ErrorMessageText__c,
    SubType__c,
    ssot__OutputValueText__c
FROM ssot__AiAgentInteractionStep__dlm
WHERE ssot__StartTimestamp__c >= '{data_inicio}'
  AND ssot__StartTimestamp__c <  '{data_fim}'
