-- fw.d_delta_mode definition

-- Drop table

-- DROP TABLE fw.d_delta_mode;

CREATE TABLE fw.d_delta_mode (
	delta_mode text NOT NULL,
	desc_short text NULL,
	desc_middle text NULL,
	desc_long text NULL,
	CONSTRAINT pk_delta_mode PRIMARY KEY (delta_mode)
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE fw.d_delta_mode OWNER TO "admin";
GRANT ALL ON TABLE fw.d_delta_mode TO "admin";