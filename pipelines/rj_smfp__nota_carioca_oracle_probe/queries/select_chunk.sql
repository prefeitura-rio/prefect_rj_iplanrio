SELECT $columns
FROM $schema.$table AS OF SCN :scn
WHERE ROWID BETWEEN CHARTOROWID(:start_rowid) AND CHARTOROWID(:end_rowid)
