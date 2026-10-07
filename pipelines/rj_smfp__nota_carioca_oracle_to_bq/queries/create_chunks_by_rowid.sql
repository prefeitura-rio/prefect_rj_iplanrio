BEGIN
    DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID(
        task_name   => :task_name,
        table_owner => :owner,
        table_name  => :table_name,
        by_row      => FALSE,
        chunk_size  => :chunk_size
    );
END;
