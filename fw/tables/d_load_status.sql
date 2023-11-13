CREATE TABLE ${target_schema}.d_load_status (
	load_status int4 NOT NULL,
	desc_short text NULL,
	desc_middle text NULL,
	desc_long text NULL,
	CONSTRAINT pk_load_status PRIMARY KEY (load_status)
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE ${target_schema}.d_load_status OWNER TO "${owner}";
GRANT ALL ON TABLE ${target_schema}.d_load_status TO "${owner}";