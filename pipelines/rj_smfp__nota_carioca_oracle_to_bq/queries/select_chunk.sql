-- AS OF SCN é obrigatório: além da foto consistente, força o acesso por faixa de ROWID; sem ele a leitura foi ~100x mais lenta em prod.
SELECT $columns
FROM $schema.$table AS OF SCN :scn
WHERE ROWID BETWEEN CHARTOROWID(:start_rowid) AND CHARTOROWID(:end_rowid)
