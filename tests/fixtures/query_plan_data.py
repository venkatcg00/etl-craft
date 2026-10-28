"""Set-based PostgreSQL run history for catalog plan and write-cost measurements."""

from sqlalchemy import inspect, text

from fixtures.upgrade_rows import historical_rows

STAMP = "TIMESTAMPTZ '2025-01-01 00:00:00+00'"
RUNS = "generate_series(1,:pipelines) p CROSS JOIN generate_series(1,:days) d"
TASKS = "generate_series(1,:pipelines) p CROSS JOIN generate_series(1,:tasks) t"
RUN_ID = "((p-1)*:days+d)"
TASK_ID = "((p-1)*:tasks+t)"
TASK_RUN_ID = f"(({RUN_ID}-1)*:tasks+t)"
DAY = f"({STAMP}+(d-1)*INTERVAL '1 day')"

# Every run has its own task/attempt identity; completed days dominate the live working set.
SEEDS = [
    (
        "AUD_CLONE_STATE",
        "MIRROR_ID,TABLE_NAME,SCHEMA_SHA256,INITIALIZED,COPIED_AT",
        f"'mirror-'||p,'table-'||p,'schema',1,{STAMP}",
        "generate_series(1,:pipelines) p",
    ),
    (
        "AUD_CLONE_CHANGES",
        "MIRROR_ID,TABLE_NAME,ROW_KEY,VERSION",
        "'mirror-'||p,'table-'||p,'['||p||']','version'",
        "generate_series(1,:pipelines) p",
    ),
    (
        "AUD_RETENTION_ARCHIVES",
        "PROJECT_ID,BATCH_ID,DESTINATION,SCHEMA_NAME,BODY,CREATED_AT",
        f"1,'batch','destination','history','{{}}',{STAMP}",
        "generate_series(1,1) p",
    ),
    (
        "AUD_RETENTION_TICKS",
        "PROJECT_ID,PURGED_ON",
        f"1,{STAMP}::date",
        "generate_series(1,1) p",
    ),
    (
        "AUD_RETENTION_FILES",
        "ATTEMPT_ID,PROJECT_ID,LOG_PATH,RECORDED_AT",
        f"p,1,'/plan/log-'||p,{STAMP}",
        "generate_series(1,:pipelines) p",
    ),
    (
        "AUD_CONNECTION_RESERVATIONS",
        "RESERVATION_ID,ROLE,HOST,PID,BIRTH_TIME,POOL_CONNECTIONS,SESSION_CONNECTIONS,LOCAL_TASK_CONNECTIONS,BACKEND_PID,BACKEND_STARTED_AT,STARTED_AT,HEARTBEAT_AT,EXPIRES_AT,STOPPED_AT,ACTOR,ACTOR_KIND",
        f"p::text,'UI','plan',p,1,10,2,0,p,{STAMP},{STAMP},{STAMP},{STAMP},{STAMP},'plan','SYSTEM'",
        "generate_series(1,:pipelines) p",
    ),
    (
        "AUD_PROJECT_BUNDLES",
        "BUNDLE_ID,MANIFEST,CREATED_BY,CREATED_AT,CONNECTIONS_APPROVED_FLAG",
        f"lpad(p::text,64,'0'),'{{}}','plan',{STAMP},'Y'",
        "generate_series(1,:pipelines) p",
    ),
    (
        "CFG_PIPELINES",
        "PIPELINE_ID,PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE,RUN_SCHEDULE",
        "p,'PLAN_'||p,'Plan '||p,'FULL','0 0 * * *'",
        "generate_series(1,:pipelines) p",
    ),
    (
        "AUD_PIPELINE_REFRESHES",
        "REFRESH_ID,PROJECT_ID,PIPELINE_ID,REFRESHED_AT,ACTOR,ACTOR_KIND,STATUS,"
        "METADATA_VERSION,METADATA_SNAPSHOT,BUNDLE_ID,FINDINGS",
        f"{RUN_ID},1,p,{DAY},'plan','SYSTEM','SUCCESS',repeat('a',64),"
        "jsonb_build_object('CFG_PIPELINES',jsonb_build_object('rows',"
        "jsonb_build_array(jsonb_build_object('pipeline_id',p,'run_schedule','0 0 * * *'))))::text,"
        "lpad(p::text,64,'0'),'[]'",
        RUNS,
    ),
    (
        "CFG_TABLES",
        "TABLE_ID,SCHEMA_NAME,TABLE_NAME",
        "p,'plan','table_'||p",
        "generate_series(1,:pipelines) p",
    ),
    (
        "CFG_TASKS",
        "TASK_ID,PIPELINE_ID,TASK_CODE,TASK_TYPE,HANDLER,TARGET_TABLE_ID",
        f"{TASK_ID},p,'task_'||t,'ETL','SQL',p",
        TASKS,
    ),
    (
        "CFG_TASK_PARAMETERS",
        "TASK_ID,PARAMETER_NAME,PARAMETER_VALUE",
        f"{TASK_ID},'SQL_ACTION','SCD1_MERGE'",
        TASKS,
    ),
    (
        "CFG_PIPELINE_DEPENDENCY",
        "PIPELINE_DEPENDENCY_ID,PIPELINE_ID,DEPENDS_ON_PIPELINE_ID,DEPENDENCY_TYPE",
        "p,p,p-1,'SUCCESS'",
        "generate_series(2,:pipelines) p",
    ),
    (
        "CFG_TASK_DEPENDENCY",
        "TASK_DEPENDENCY_ID,PIPELINE_ID,TASK_ID,DEPENDS_ON_PIPELINE_ID,DEPENDS_ON_TASK_ID,DEPENDENCY_TYPE",
        f"{TASK_ID},p,{TASK_ID},p,{TASK_ID}-1,'SUCCESS'",
        TASKS + " WHERE t>1",
    ),
    (
        "CFG_BUSINESS_RULES",
        "BUSINESS_RULE_ID,BUSINESS_RULE_NAME,PIPELINE_ID,TASK_ID,BUSINESS_RULE_TYPE,BUSINESS_RULE_KEY_COLUMN,BUSINESS_RULE_SQL,TARGET_TABLE_ID,SEQUENCE_NUMBER",
        "p,'rule_'||p,p,(p-1)*:tasks+1,'REPORT','id','SELECT 1 AS id',p,1",
        "generate_series(1,:pipelines) p",
    ),
    (
        "AUD_PIPELINES_RUN_LOG",
        "PIPELINE_RUN_ID,PIPELINE_ID,START_DATE,END_DATE,STATUS,RUN_KEY,TRIGGER_KIND",
        f"{RUN_ID},p,{DAY},{DAY}+INTERVAL '1 hour',"
        "CASE WHEN d=:days AND p%100=1 THEN 'QUEUED' ELSE 'SUCCESS' END,"
        f"'schedule:'||{DAY}::text,'SCHEDULE'",
        RUNS,
    ),
    (
        "AUD_TASK_RUN_LOG",
        "TASK_RUN_ID,TASK_ID,PIPELINE_RUN_ID,START_DATE,END_DATE,STATUS",
        f"{TASK_RUN_ID},{TASK_ID},{RUN_ID},{DAY},{DAY}+INTERVAL '1 minute','SUCCESS'",
        RUNS + " CROSS JOIN generate_series(1,:tasks) t",
    ),
    (
        "AUD_TASK_ATTEMPTS",
        "ATTEMPT_ID,TASK_RUN_ID,ATTEMPT_NUMBER,STATUS,OWNER_ID,STARTED_AT,ENDED_AT,LEASE_EXPIRES_AT,FAILURE_SOURCE",
        f"{TASK_RUN_ID},{TASK_RUN_ID},1,"
        "CASE WHEN d=:days AND p%100=1 AND t=1 THEN 'QUEUED' ELSE 'SUCCESS' END,"
        f"'plan',{DAY},{DAY}+INTERVAL '1 minute',{DAY}+INTERVAL '1 minute',"
        "CASE WHEN d%10=0 THEN 'WAREHOUSE' END",
        RUNS + " CROSS JOIN generate_series(1,:tasks) t",
    ),
    (
        "AUD_GATE_DECISIONS",
        "PIPELINE_RUN_ID,PIPELINE_DEPENDENCY_ID,SELECTED_PIPELINE_RUN_ID,RESULT,REASON,DECIDED_AT",
        f"{RUN_ID},p,{RUN_ID}-:days,'SATISFIED','plan',{DAY}",
        RUNS + " WHERE p>1",
    ),
    (
        "AUD_DEPENDENCY_CONSUMPTION",
        "PIPELINE_DEPENDENCY_ID,PIPELINE_ID,PIPELINE_RUN_ID,DEPENDS_ON_PIPELINE_ID,CONSUMED_PIPELINE_RUN_ID,CONSUMED_AT",
        f"p,p,{RUN_ID},p-1,{RUN_ID}-:days,{DAY}",
        RUNS + " WHERE p>1",
    ),
    (
        "AUD_GATE_DECISIONS",
        "PIPELINE_RUN_ID,ATTEMPT_ID,TASK_DEPENDENCY_ID,SELECTED_PIPELINE_RUN_ID,SELECTED_TASK_RUN_ID,RESULT,REASON,DECIDED_AT",
        f"{RUN_ID},({RUN_ID}-1)*:tasks+2,(p-1)*:tasks+2,{RUN_ID},({RUN_ID}-1)*:tasks+1,'SATISFIED','plan',{DAY}",
        RUNS,
    ),
    (
        "AUD_DEPENDENCY_CONSUMPTION",
        "TASK_DEPENDENCY_ID,PIPELINE_ID,PIPELINE_RUN_ID,TASK_ID,DEPENDS_ON_PIPELINE_ID,CONSUMED_PIPELINE_RUN_ID,CONSUMED_TASK_RUN_ID,CONSUMED_AT",
        f"(p-1)*:tasks+2,p,{RUN_ID},(p-1)*:tasks+2,p,{RUN_ID},({RUN_ID}-1)*:tasks+1,{DAY}",
        RUNS,
    ),
    (
        "AUD_ACTIONS",
        "PIPELINE_ID,PIPELINE_RUN_ID,STARTED_AT,ENDED_AT,ACTOR,ACTOR_KIND,HOST,COMMAND,ARGUMENTS,OUTCOME",
        f"p,{RUN_ID},{DAY},{DAY},'plan','SYSTEM','plan','run','{{}}','SUCCESS'",
        RUNS,
    ),
    (
        "AUD_RUN_INTERVENTIONS",
        "PIPELINE_ID,PIPELINE_RUN_ID,ACTION,REASON,REQUESTED_BY,REQUESTED_AT",
        f"p,{RUN_ID},'MARK','plan','plan',{DAY}",
        RUNS,
    ),
    (
        "AUD_PIPELINE_PAUSES",
        "PIPELINE_ID,PAUSED_AT,PAUSED_BY,REASON,RESUMED_AT",
        f"p,{DAY},'plan','plan',{DAY}+INTERVAL '1 minute'",
        RUNS,
    ),
    (
        "AUD_CONTROL_REQUESTS",
        "REQUEST_KEY,PIPELINE_RUN_ID,ACTION,STATUS,ATTEMPT_IDS,REQUESTED_BY,REQUESTED_BY_KIND,REASON,REQUESTED_AT,MESSAGE",
        f"'plan:'||{RUN_ID},{RUN_ID},'HOLD','COMPLETED','[]','plan','SYSTEM','plan',{DAY},'plan'",
        RUNS,
    ),
    (
        "AUD_GATE_WAITS",
        "PIPELINE_RUN_ID,FIRST_CHECK_AT,NEXT_CHECK_AT,LOOKS,WAIT_UNTIL",
        f"{RUN_ID},{DAY},{DAY},1,{DAY}+INTERVAL '1 hour'",
        RUNS,
    ),
    (
        "AUD_CONTROLLERS",
        "HOST,PID,VERSION,STARTED_AT,HEARTBEAT_AT,STOPPED_AT,STARTED_BY,STARTED_BY_KIND",
        f"'plan',1,'plan',{DAY},{DAY},{DAY},'plan','SYSTEM'",
        RUNS,
    ),
    (
        "AUD_BUSINESS_RULES_RUN_LOG",
        "BUSINESS_RULE_RUN_ID,BUSINESS_RULE_ID,TASK_RUN_ID,STATUS",
        f"{RUN_ID},p,({RUN_ID}-1)*:tasks+1,'SUCCESS'",
        RUNS,
    ),
    (
        "AUD_BUSINESS_RULES_RESULTS",
        "BUSINESS_RULE_RUN_ID,BUSINESS_RULE_ID,BUSINESS_RULE_KEY,STATUS,TARGET_SCHEMA_NAME,TARGET_TABLE_NAME",
        f"{RUN_ID},p,'key_'||d,'REPORT','plan','table_'||p",
        RUNS,
    ),
    (
        "AUD_TASK_OFFSET_TRACKER",
        "TASK_ID,OFFSET_TYPE,OFFSET_VALUE",
        f"{TASK_ID},'NUMBER','17'",
        TASKS,
    ),
    (
        "AUD_COLUMN_LINEAGE",
        "TASK_ID,SOURCE_SQL_HASH,TARGET_SCHEMA_NAME,TARGET_TABLE_NAME,TARGET_COLUMN",
        f"{TASK_ID},'plan','plan','table_'||p,'column_'||t",
        TASKS,
    ),
    (
        "AUD_LINEAGE_VERSIONS",
        "LINEAGE_VERSION_ID,TARGET_SCHEMA_NAME,TARGET_TABLE_NAME,VERSION_NUMBER,EDGES_HASH,VALID_FROM,VALID_TO,CURRENT_FLAG",
        f"{RUN_ID},'plan','table_'||p,d,'plan',{DAY},{DAY}+INTERVAL '1 day',"
        "CASE WHEN d=:days THEN 'Y' ELSE 'N' END",
        RUNS,
    ),
    (
        "AUD_LINEAGE_VERSION_EDGES",
        "LINEAGE_VERSION_ID,TASK_ID,TARGET_COLUMN",
        f"{RUN_ID},(p-1)*:tasks+1,'id'",
        RUNS,
    ),
    (
        "AUD_TASK_DOCUMENTATION",
        "TASK_ID,VERSION,DOCUMENTATION_HASH,DOCUMENTATION",
        "(p-1)*:tasks+1,d,'plan','plan'",
        RUNS,
    ),
    (
        "AUD_DOCS_PUBLICATION",
        "PUBLISHED_URL",
        "'https://plan.example/'||p",
        "generate_series(1,:pipelines) p",
    ),
    (
        "AUD_TARGET_HASH_VERSION",
        "TARGET_CATALOG_NAME,TARGET_SCHEMA_NAME,TARGET_TABLE_NAME,HASH_VERSION,RECOMPUTED_AT",
        f"'','plan','table_'||p,2,{STAMP}",
        "generate_series(1,:pipelines) p",
    ),
    (
        "AUD_TARGET_VIEW_STATEMENT",
        "TARGET_CATALOG_NAME,TARGET_SCHEMA_NAME,TARGET_TABLE_NAME,STATEMENT_SHA256,RECORDED_AT",
        f"'','plan','table_'||p,'plan',{STAMP}",
        "generate_series(1,:pipelines) p",
    ),
    (
        "AUD_TASK_EVENTS",
        "TASK_RUN_ID,EVIDENCE",
        f"{TASK_RUN_ID},'{{}}'",
        RUNS + " CROSS JOIN generate_series(1,1) t",
    ),
    (
        "AUD_EVENT_FILES",
        "TASK_ID,FILE_PATH,TASK_RUN_ID",
        f"{TASK_ID},'plan/'||d,{TASK_RUN_ID}",
        RUNS + " CROSS JOIN generate_series(1,1) t",
    ),
    (
        "AUD_TASK_ITEMS",
        "REPORT_ID,TASK_RUN_ID,ATTEMPT_ID,ITEM_KEY,ITEM_ATTEMPT_NUMBER,STATUS,ROW_COUNT,CREATED_BY,CREATED_AT",
        f"'item-'||{TASK_RUN_ID},{TASK_RUN_ID},{TASK_RUN_ID},'page',1,'SUCCESS',1,'plan',{DAY}",
        RUNS + " CROSS JOIN generate_series(1,1) t",
    ),
    (
        "AUD_EVENT_KEYS",
        "TASK_ID,EVENT_KEY,TASK_RUN_ID,CREATED_BY,CREATED_AT",
        f"{TASK_ID},'signal/'||d,{TASK_RUN_ID},'plan',{DAY}",
        RUNS + " CROSS JOIN generate_series(1,1) t",
    ),
    (
        "AUD_ATTEMPT_ACKNOWLEDGEMENTS",
        "ATTEMPT_ID,KIND,OBSERVED_AT",
        f"{TASK_RUN_ID},'PROCESS',{DAY}",
        RUNS + " CROSS JOIN generate_series(1,1) t",
    ),
    (
        "AUD_SOURCE_COLUMNS_BASELINE",
        "SOURCE_COLUMN_ID,TABLE_NAME,COLUMN_NAME,DATA_TYPE,COLUMN_POSITION,CREATED_BY,CREATE_DATE,UPDATED_BY,UPDATED_DATE",
        f"{TASK_ID},'plan.table_'||p,'column_'||t,'INTEGER',t,'plan',{STAMP},'plan',{STAMP}",
        TASKS,
    ),
    (
        "CFG_ACCOUNTS",
        "ACCOUNT_ID,ACCOUNT_NAME,ACCOUNT_KIND,ROLE",
        "p,'agent_'||p,'SERVICE','viewer'",
        "generate_series(1,:pipelines) p",
    ),
    (
        "CFG_API_TOKENS",
        "TOKEN_ID,NAME,TOKEN_SHA256,ACCOUNT_ID,TOKEN_KIND",
        "p,'agent_'||p,'hash_'||p,p,'AGENT'",
        "generate_series(1,:pipelines) p",
    ),
    (
        "AUD_AGENTS",
        "AGENT_ID,POOL,HOST,VERSION,PYTHON_SLOTS,WAREHOUSE_SLOTS,STARTED_AT,"
        "HEARTBEAT_AT,STATUS,RESOURCE_REPORT",
        f"p,'default','host_'||p,'test',1,1,{STAMP},{STAMP},'GONE',"
        "json_build_object('node_id','node_'||p)::text",
        "generate_series(1,:pipelines) p",
    ),
    (
        "AUD_AGENT_COMMANDS",
        "AGENT_ID,COMMAND,ACTOR,ACTOR_KIND,REQUESTED_AT,ACKNOWLEDGED_AT",
        f"p,'DRAIN','plan','SYSTEM',{DAY},{DAY}",
        RUNS,
    ),
    (
        "AUD_RESOURCE_LOCKS",
        "LOCK_KEY,RESOURCE_NAME,HOLDER_ID,ATTEMPT_ID,OWNER_ID,ACTOR,"
        "ACTOR_KIND,ACQUIRED_AT,LEASE_EXPIRES_AT",
        f"{TASK_RUN_ID},'target_'||p,'attempt:'||{TASK_RUN_ID},{TASK_RUN_ID},'plan','plan',"
        f"'SYSTEM',{DAY},{DAY}+interval '1 minute'",
        RUNS + " CROSS JOIN generate_series(1,1) t",
    ),
    (
        "AUD_ATTEMPT_LOG_CHUNKS",
        "ATTEMPT_ID,SEQUENCE_NUMBER,CONTENT_SHA256,CONTENT,BYTE_OFFSET,BYTE_LENGTH,CREATED_AT",
        f"{TASK_RUN_ID},0,'plan','text',0,4,{DAY}",
        RUNS + " CROSS JOIN generate_series(1,1) t",
    ),
]


def seed_query_plans(conn, *, pipelines=1000, tasks=20, days=10):
    inspector = inspect(conn)
    tables = inspector.get_table_names()
    values = {"pipelines": pipelines, "tasks": tasks, "days": days}
    with historical_rows(conn, tables), conn.begin_nested():
        for table, columns, expressions, source in SEEDS:
            if table.startswith("CFG_"):
                columns += ",CREATED_BY,CREATE_DATE,UPDATED_BY,UPDATED_DATE"
                expressions += f",'plan',{STAMP},'plan',{STAMP}"
            batch = max(1, min(pipelines, 1000000 // (tasks * days)))
            source = source.replace(
                "generate_series(1,:pipelines)",
                "generate_series(CAST(:first_pipeline AS INTEGER),CAST(:last_pipeline AS INTEGER))",
            )
            source = source.replace(
                "generate_series(2,:pipelines)",
                "generate_series(GREATEST(2,CAST(:first_pipeline AS INTEGER)),"
                "CAST(:last_pipeline AS INTEGER))",
            )
            for first in range(1, pipelines + 1, batch):
                chunk = values | {
                    "first_pipeline": first,
                    "last_pipeline": min(pipelines, first + batch - 1),
                }
                conn.execute(
                    text(
                        f"INSERT INTO {table} ({columns}) OVERRIDING SYSTEM VALUE "
                        f"SELECT {expressions} FROM {source}"
                    ),
                    chunk,
                )
        for number, table in enumerate(
            sorted(name for name in tables if name.endswith("_history")), 1
        ):
            conn.execute(
                text(
                    f"INSERT INTO {table} (CHANGE_ID,VALID_FROM,VALID_TO,CURRENT_FLAG,"
                    f"OPERATION,ACTOR,ACTOR_KIND) SELECT {number}*1000000000::bigint+n,"
                    f"{STAMP},{STAMP}+INTERVAL '1 day','N','UPDATE','plan','SYSTEM' "
                    "FROM generate_series(1,:count) n"
                ),
                {"count": pipelines * days},
            )
    conn.exec_driver_sql("ANALYZE")
