{#
{
  "kind": "template",
  "description": "Set the time zone of the session, which Oracle uses to compare a time stamp with a time zone to a date.",
  "inputs": {
    "zone": "Literal with the name of the time zone."
  }
}
#}
-- noqa: disable=PRS
ALTER SESSION SET TIME_ZONE = {{ zone }}
