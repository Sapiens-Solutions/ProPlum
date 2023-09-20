-- fw.d_load_method definition

-- Drop table

-- DROP TABLE fw.d_load_method;

CREATE TABLE fw.d_load_method (
	load_method text NOT NULL,
	desc_short text NULL,
	desc_middle text NULL,
	desc_long text NULL,
	CONSTRAINT pk_load_method PRIMARY KEY (load_method)
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE fw.d_load_method OWNER TO "admin";
GRANT ALL ON TABLE fw.d_load_method TO "admin";