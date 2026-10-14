{#
{
  "kind": "template",
  "description": "Create the secret that holds the credentials of an Oracle database. The password is bound as the only parameter.",
  "inputs": {
    "secret": "Secret identifier.",
    "host": "Host literal.",
    "port": "Port literal.",
    "service": "Oracle service name literal.",
    "user": "User literal."
  }
}
#}
-- noqa: disable=PRS
CREATE OR REPLACE SECRET {{ secret }} (
    TYPE oracle,
    HOST {{ host }},
    PORT {{ port }},
    SERVICE_NAME {{ service }},
    USER {{ user }},
    PASSWORD ?
)
