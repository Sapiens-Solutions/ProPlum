-- fw.ext_tables_params definition

-- Drop table

-- DROP TABLE fw.ext_tables_params;

CREATE TABLE fw.ext_tables_params (
	object_id int8 NULL,
	load_method text NULL,
	connection_string text NULL,
	additional text NULL,
	"active" bool NULL
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE fw.ext_tables_params OWNER TO "admin";
GRANT ALL ON TABLE fw.ext_tables_params TO "admin";