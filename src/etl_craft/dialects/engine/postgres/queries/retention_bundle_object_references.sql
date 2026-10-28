SELECT COUNT(*) AS references_count FROM AUD_PROJECT_BUNDLES
WHERE BUNDLE_ID<>:bundle_id AND jsonb_path_query_array(MANIFEST::jsonb,'$.files.*') @> CAST(:digests AS jsonb)
