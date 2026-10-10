SELECT /*+ PARALLEL($degree) */ COUNT(*)
FROM "$schema"."$table"
