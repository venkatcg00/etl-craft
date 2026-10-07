SELECT FIRST_CHECK_AT AS first_check_at, NEXT_CHECK_AT AS next_check_at,
       LOOKS AS looks, WAIT_UNTIL AS wait_until FROM AUD_GATE_WAITS
WHERE PIPELINE_RUN_ID=:pipeline_run_id AND COALESCE(TASK_ID,0)=COALESCE(:task_id,0)
