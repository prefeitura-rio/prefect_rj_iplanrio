-- Agentforce (STDM). Colunas verificadas via SELECT * LIMIT 1 em 2026-07-23.
-- Filtra por MessageSentTimestamp (não StartTimestamp como as outras 3).
-- Janela {{ start }}/{{ end }}: hora-parede de SP com rótulo Z (ver utils/janela.py).
SELECT
    ssot__Id__c,
    ssot__AiAgentInteractionId__c,
    ssot__AiAgentSessionId__c,
    ssot__AiAgentSessionParticipantId__c,
    ssot__AiAgentInteractionMessageType__c,
    ssot__AiAgentInteractionMsgContentType__c,
    ssot__SessionOwnerId__c,
    ssot__IndividualId__c,
    ssot__ParentMessageId__c,
    ssot__ContentText__c,
    ssot__MessageSentTimestamp__c,
    MessageStartTimestamp__c,
    MessageEndTimestamp__c,
    ssot__InternalOrganizationId__c,
    ssot__DataSourceId__c,
    ssot__DataSourceObjectId__c,
    ssot__ExternalSourceId__c,
    Modality__c
FROM ssot__AiAgentInteractionMessage__dlm
WHERE ssot__MessageSentTimestamp__c >= '{{ start }}'
  AND ssot__MessageSentTimestamp__c <  '{{ end }}'
