SELECT $columns
FROM $schema.$table
WHERE ROWID BETWEEN CHARTOROWID(:start_rowid) AND CHARTOROWID(:end_rowid)
