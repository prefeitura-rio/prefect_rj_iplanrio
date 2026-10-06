SELECT 1 AS present
FROM $schema.$table AS OF SCN :scn
WHERE ROWNUM = 1
