SELECT upper(tablename) AS table_name FROM pg_indexes WHERE schemaname=current_schema() AND lower(indexname)=lower(:index_name)
