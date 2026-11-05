SELECT
    tablespace_name,
    COUNT(*)                                  AS arquivos,
    ROUND(SUM(bytes) / 1073741824, 2)         AS tamanho_gb,
    ROUND(SUM(maxbytes) / 1073741824, 2)      AS tamanho_max_gb,
    MAX(autoextensible)                       AS autoextend
FROM dba_data_files
WHERE tablespace_name IN (SELECT tablespace_name FROM dba_tablespaces WHERE contents = 'UNDO')
GROUP BY tablespace_name
ORDER BY tablespace_name
