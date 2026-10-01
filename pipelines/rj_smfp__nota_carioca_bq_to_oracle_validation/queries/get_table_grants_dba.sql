SELECT grantee, privilege
FROM dba_tab_privs
WHERE owner = :owner
    AND table_name = :table_name
ORDER BY grantee, privilege
