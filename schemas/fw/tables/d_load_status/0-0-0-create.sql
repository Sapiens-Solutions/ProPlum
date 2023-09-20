-- fw.d_load_status definition

-- Drop table

-- DROP TABLE fw.d_load_status;

CREATE TABLE fw.d_load_status (
	load_status int4 NOT NULL,
	desc_short text NULL,
	desc_middle text NULL,
	desc_long text NULL,
	CONSTRAINT pk_load_status PRIMARY KEY (load_status)
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE fw.d_load_status OWNER TO "admin";
GRANT ALL ON TABLE fw.d_load_status TO "admin";