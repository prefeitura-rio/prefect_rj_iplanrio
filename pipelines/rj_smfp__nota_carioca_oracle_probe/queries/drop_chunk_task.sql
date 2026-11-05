BEGIN
    DBMS_PARALLEL_EXECUTE.DROP_TASK(task_name => :task_name);
END;
