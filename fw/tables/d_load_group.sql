CREATE TABLE ${target_schema}.d_load_group (
	load_group text NOT NULL,
	desc_short text NULL,
	desc_middle text NULL,
	desc_long text NULL,
	CONSTRAINT pk_load_group PRIMARY KEY (load_group)
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE ${target_schema}.d_load_group OWNER TO "${owner}";
GRANT ALL ON TABLE ${target_schema}.d_load_group TO "${owner}";