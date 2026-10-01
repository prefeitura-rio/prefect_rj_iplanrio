SELECT owner, table_owner, table_name
FROM all_synonyms
WHERE synonym_name = :synonym_name
