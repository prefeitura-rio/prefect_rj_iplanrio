SELECT
    Id,
    ConversationId,
    Status,
    StartTime,
    EndTime,
    MessagingChannelId,
    MessagingEndUserId,
    Origin,
    CreatedDate,
    LastModifiedDate
FROM MessagingSession
WHERE LastModifiedDate >= {{ start }}
  AND LastModifiedDate < {{ end }}
ORDER BY CreatedDate ASC
