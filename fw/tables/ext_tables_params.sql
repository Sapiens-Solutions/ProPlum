CREATE TABLE ${target_schema}.ext_tables_params (
	object_id int8 NULL,
	load_method text NULL,
	connection_string text NULL,
	additional text NULL,
	active bool NULL
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE ${target_schema}.ext_tables_params OWNER TO "${owner}";
GRANT ALL ON TABLE ${target_schema}.ext_tables_params TO "${owner}";