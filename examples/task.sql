SELECT COUNT(1) FROM
(
    SELECT std.task_id FROM some_task_detail std WHERE std.STATUS = 1) a
JOIN (
    SELECT st.task_id FROM some_task st WHERE task_type_id = 80) b
ON a.task_id = b.task_id;