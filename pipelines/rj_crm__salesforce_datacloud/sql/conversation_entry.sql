-- Messaging (Data Cloud).
-- Janela {data_inicio}/{data_fim}: hora-parede de SP com rótulo Z (ver utils/janela.py).
SELECT
    ssot__Id__c,
    ssot__ConversationId__c,
    ssot__ConversationEntryType__c,
    ssot__ConversationEntryVisibilityType__c,
    ssot__EngagementParticipantId__c,
    ssot__PayloadText__c,
    ssot__Language__c,
    ssot__DurationSecondsCount__c,
    ssot__VersionNumber__c,
    ssot__ExternalRecordId__c,
    ssot__ClientDateTime__c,
    ssot__TranscriptedDateTime__c,
    ssot__CreatedDate__c,
    ssot__LastModifiedDate__c,
    ssot__InternalOrganizationId__c,
    ssot__DataSourceId__c,
    ssot__DataSourceObjectId__c,
    KQ_Id__c
FROM ssot__ConversationEntry__dlm
WHERE ssot__CreatedDate__c >= '{data_inicio}'
  AND ssot__CreatedDate__c <  '{data_fim}'
