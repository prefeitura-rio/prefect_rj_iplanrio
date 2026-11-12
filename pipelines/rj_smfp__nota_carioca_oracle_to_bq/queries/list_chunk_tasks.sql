SELECT task_name
FROM user_parallel_execute_tasks
WHERE task_name LIKE :pattern ESCAPE '\'
ORDER BY task_name
