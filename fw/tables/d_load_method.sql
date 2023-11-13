CREATE TABLE ${target_schema}.d_load_method (
	load_method text NOT NULL,
	desc_short text NULL,
	desc_middle text NULL,
	desc_long text NULL,
	CONSTRAINT pk_load_method PRIMARY KEY (load_method)
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE ${target_schema}.d_load_method OWNER TO "${owner}";
GRANT ALL ON TABLE ${target_schema}.d_load_method TO "${owner}";