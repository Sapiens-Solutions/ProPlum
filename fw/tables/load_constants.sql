CREATE TABLE ${target_schema}.load_constants (
	constant_name text NULL,
	constant_type text NULL,
	constant_value text NULL
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE ${target_schema}.load_constants OWNER TO "${owner}";
GRANT ALL ON TABLE ${target_schema}.load_constants TO "${owner}";