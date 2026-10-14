{#
{
  "kind": "template",
  "description": "Create the secret that holds the credentials of a source database. The password is bound as the only parameter.",
  "inputs": {
    "secret": "Secret identifier.",
    "type": "Scanner type: postgres, mysql or mssql.",
    "host": "Host literal.",
    "port": "Port literal.",
    "database": "Database literal.",
    "user": "User literal."
  }
}
#}
-- noqa: disable=PRS
CREATE OR REPLACE SECRET {{ secret }} (
    TYPE {{ type }},
    HOST {{ host }},
    PORT {{ port }},
    DATABASE {{ database }},
    USER {{ user }},
    PASSWORD ?
)
