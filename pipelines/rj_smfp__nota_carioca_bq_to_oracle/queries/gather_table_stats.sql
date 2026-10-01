BEGIN
    DBMS_STATS.GATHER_TABLE_STATS(ownname => :owner, tabname => :table_name, degree => :degree, cascade => FALSE);
END;
