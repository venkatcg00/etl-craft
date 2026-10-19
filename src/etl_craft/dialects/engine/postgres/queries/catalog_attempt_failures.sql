SELECT TASK_RUN_ID AS task_run_id, ATTEMPT_NUMBER AS attempt_number,
       FAILURE_CLASS AS failure_class, FAILURE_SOURCE AS failure_source,
       FAILURE_CODE AS failure_code, RETRY_ON AS retry_on, RETRYABLE AS retryable
FROM AUD_TASK_ATTEMPTS
WHERE TASK_RUN_ID IN (SELECT CAST(value AS BIGINT) AS task_run_id FROM jsonb_array_elements_text(CAST(:task_run_ids AS JSONB))) AND FAILURE_SOURCE IS NOT NULL
ORDER BY TASK_RUN_ID, ATTEMPT_NUMBER
