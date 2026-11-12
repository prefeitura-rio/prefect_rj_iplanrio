SELECT table_name AS pacote, grantee, privilege AS privilegio
FROM all_tab_privs
WHERE table_schema = 'SYS'
  AND table_name IN ('DBMS_FLASHBACK', 'DBMS_PARALLEL_EXECUTE')
ORDER BY table_name, grantee
