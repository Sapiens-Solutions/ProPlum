CREATE TABLE ${target_schema}.d_delta_mode (
	delta_mode text NOT NULL,
	desc_short text NULL,
	desc_middle text NULL,
	desc_long text NULL,
	CONSTRAINT pk_delta_mode PRIMARY KEY (delta_mode)
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE ${target_schema}.d_delta_mode OWNER TO "${owner}";
GRANT ALL ON TABLE ${target_schema}.d_delta_mode TO "${owner}";