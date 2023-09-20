-- fw.load_constants definition

-- Drop table

-- DROP TABLE fw.load_constants;

CREATE TABLE fw.load_constants (
	constant_name text NULL,
	constant_type text NULL,
	constant_value text NULL
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE fw.load_constants OWNER TO "admin";
GRANT ALL ON TABLE fw.load_constants TO "admin";