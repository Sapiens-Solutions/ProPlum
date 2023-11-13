CREATE TABLE ${target_schema}.d_load_type (
	load_type text NOT NULL,
	desc_short text NULL,
	desc_middle text NULL,
	desc_long text NULL,
	CONSTRAINT pk_load_type PRIMARY KEY (load_type)
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE ${target_schema}.d_load_type OWNER TO "${owner}";
GRANT ALL ON TABLE ${target_schema}.d_load_type TO "${owner}";