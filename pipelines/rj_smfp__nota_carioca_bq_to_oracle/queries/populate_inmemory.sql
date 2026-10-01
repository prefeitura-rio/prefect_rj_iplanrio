BEGIN
    DBMS_INMEMORY.POPULATE(schema_name => :owner, table_name => :table_name, partition_name => :partition_name);
END;
