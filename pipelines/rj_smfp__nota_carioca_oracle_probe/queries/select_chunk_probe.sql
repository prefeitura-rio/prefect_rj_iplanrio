SELECT 1 AS present
FROM $schema.$table
WHERE ROWID BETWEEN CHARTOROWID(:start_rowid) AND CHARTOROWID(:end_rowid)
  AND ROWNUM = 1
