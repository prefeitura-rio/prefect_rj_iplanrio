SELECT
    sys_context('USERENV', 'SESSION_USER') AS session_user,
    sys_context('USERENV', 'PROXY_USER') AS proxy_user,
    sys_context('USERENV', 'DB_NAME') AS db_name,
    sys_context('USERENV', 'SERVICE_NAME') AS service_name,
    (SELECT version FROM product_component_version WHERE product LIKE 'Oracle%' AND ROWNUM = 1) AS db_version,
    TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS TZH:TZM') AS db_time
FROM dual
