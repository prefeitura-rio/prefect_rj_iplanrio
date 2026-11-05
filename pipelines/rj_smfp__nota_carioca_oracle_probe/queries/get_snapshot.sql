SELECT
    current_scn AS scn,
    SYS_EXTRACT_UTC(SYSTIMESTAMP) AS taken_at
FROM v$$database
