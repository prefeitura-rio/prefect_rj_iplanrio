-- Tracing.
-- Janela {data_inicio}/{data_fim}: hora-parede de SP com rótulo Z (ver utils/janela.py).
SELECT
    ssot__Id__c,
    ssot__TelemetryTrace__c,
    ssot__TelemetryParentSpanId__c,
    ssot__OperationName__c,
    ssot__SpanKind__c,
    ssot__StartDateTime__c,
    ssot__EndDateTime__c,
    ssot__DurationNumber__c,
    ssot__StatusCode__c,
    ssot__ServiceName__c,
    ssot__TelemetrySpanAttributeText__c,
    ssot__DataSourceId__c,
    ssot__DataSourceObjectId__c,
    ssot__InternalOrganizationId__c,
    KQ_Id__c
FROM ssot__TelemetryTraceSpan__dlm
WHERE ssot__StartDateTime__c >= '{data_inicio}'
  AND ssot__StartDateTime__c <  '{data_fim}'
