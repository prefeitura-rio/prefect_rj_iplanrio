SELECT
    Id,
    Name,
    MessagingChannelId,
    MessageType,
    MessagingPlatformKey,
    Locale,
    IsoCountryCode,
    MessagingConsentStatus,
    IsFullyOptedIn,
    MessagingExternalUserKey,
    CreatedDate,
    LastModifiedDate
FROM MessagingEndUser
WHERE LastModifiedDate >= {{ start }}
  AND LastModifiedDate < {{ end }}
ORDER BY CreatedDate ASC
