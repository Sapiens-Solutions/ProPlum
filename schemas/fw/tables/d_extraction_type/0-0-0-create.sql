-- fw.d_extraction_type definition

-- Drop table

-- DROP TABLE fw.d_extraction_type;

CREATE TABLE fw.d_extraction_type (
	extraction_type text NOT NULL,
	desc_short text NULL,
	desc_middle text NULL,
	desc_long text NULL,
	CONSTRAINT pk_extraction_type PRIMARY KEY (extraction_type)
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE fw.d_extraction_type OWNER TO "admin";
GRANT ALL ON TABLE fw.d_extraction_type TO "admin";