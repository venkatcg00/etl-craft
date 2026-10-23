INSERT INTO AUD_TASK_ITEMS
(REPORT_ID,TASK_RUN_ID,ATTEMPT_ID,ITEM_KEY,ITEM_ATTEMPT_NUMBER,STATUS,
STARTED_AT,ENDED_AT,ROW_COUNT,ERROR_MESSAGE,CREATED_BY,CREATED_AT)
VALUES (:report_id,:task_run_id,:attempt_id,:key,:number,:status,
:started,:ended,:row_count,:error,:actor,:now)
