CREATE TABLE ${target_schema}.odata_query_helper (
	load_id int8 NOT NULL,
	sql_query text NOT NULL,
	table_to text NOT NULL,
	rows_count int8 NOT NULL,
	delta_field text NULL,
	extraction_to timestamp NULL

)
DISTRIBUTED BY (load_id)


-- Permissions

ALTER TABLE ${target_schema}.odata_query_helper OWNER TO "${owner}";
GRANT INSERT, TRIGGER, REFERENCES, TRUNCATE, DELETE, UPDATE, SELECT ON TABLE ${target_schema}.odata_query_helper TO "${owner}";
