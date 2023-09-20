-- fw.d_load_type definition

-- Drop table

-- DROP TABLE fw.d_load_type;

CREATE TABLE fw.d_load_type (
	load_type text NOT NULL,
	desc_short text NULL,
	desc_middle text NULL,
	desc_long text NULL,
	CONSTRAINT pk_load_type PRIMARY KEY (load_type)
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE fw.d_load_type OWNER TO "admin";
GRANT ALL ON TABLE fw.d_load_type TO "admin";