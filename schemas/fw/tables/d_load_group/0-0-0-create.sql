-- fw.d_load_group definition

-- Drop table

-- DROP TABLE fw.d_load_group;

CREATE TABLE fw.d_load_group (
	load_group text NOT NULL,
	desc_short text NULL,
	desc_middle text NULL,
	desc_long text NULL,
	CONSTRAINT pk_load_group PRIMARY KEY (load_group)
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE fw.d_load_group OWNER TO "admin";
GRANT ALL ON TABLE fw.d_load_group TO "admin";