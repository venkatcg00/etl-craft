SELECT pid AS backend_pid,backend_start AS backend_started FROM pg_stat_activity WHERE pid=pg_backend_pid()
