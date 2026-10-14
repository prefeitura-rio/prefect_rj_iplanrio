{#
{
  "kind": "template",
  "description": "Read the current system change number, which fixes the point that a dump reads.",
  "inputs": {
    "flashback": "True to read it through DBMS_FLASHBACK, false for v$database. A user needs a grant for either."
  }
}
#}
-- noqa: disable=PRS
{% if flashback %}
SELECT CAST(DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER AS NUMBER(18)) AS "scn" FROM dual
{% else %}
SELECT CAST(current_scn AS NUMBER(18)) AS "scn" FROM v$database
{% endif %}
