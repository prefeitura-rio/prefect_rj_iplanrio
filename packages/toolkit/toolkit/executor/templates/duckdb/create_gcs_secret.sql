{#
{
  "kind": "template",
  "description": "Create the GCS access secret from HMAC keys. The key id and the secret are bound as the two parameters.",
  "inputs": {}
}
#}
-- noqa: disable=PRS
CREATE SECRET (TYPE gcs, KEY_ID ?, SECRET ?)
