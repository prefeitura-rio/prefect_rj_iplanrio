{#
{
  "kind": "template",
  "description": "Create a secret and attach a source database as an alias.",
  "inputs": {
    "secret": "Secret identifier.",
    "alias": "Database alias identifier.",
    "type": "Scanner type: postgres, mysql or mssql.",
    "host": "Host literal.",
    "port": "Port literal.",
    "database": "Database literal.",
    "user": "User literal.",
    "password": "Password literal.",
    "read_only": "Whether to attach read-only."
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
    PASSWORD {{ password }}
);
ATTACH '' AS {{ alias }} (TYPE {{ type }}, SECRET {{ secret }}{% if read_only %}, READ_ONLY{% endif %})
