BEGIN
    DBMS_PARALLEL_EXECUTE.CREATE_TASK(task_name => :task_name);
END;
