CREATE TABLE ${target_schema}.d_extraction_type (
	extraction_type text NOT NULL,
	desc_short text NULL,
	desc_middle text NULL,
	desc_long text NULL,
	CONSTRAINT pk_extraction_type PRIMARY KEY (extraction_type)
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE ${target_schema}.d_extraction_type OWNER TO "${owner}";
GRANT ALL ON TABLE ${target_schema}.d_extraction_type TO "${owner}";