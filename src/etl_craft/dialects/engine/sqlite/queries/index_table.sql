SELECT upper(tbl_name) AS table_name FROM sqlite_schema WHERE type='index' AND lower(name)=lower(:index_name)
