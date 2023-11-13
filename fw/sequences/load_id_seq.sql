CREATE SEQUENCE ${target_schema}.load_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	NO CYCLE;

-- Permissions

ALTER SEQUENCE ${target_schema}.load_id_seq OWNER TO "${owner}";
GRANT ALL ON SEQUENCE ${target_schema}.load_id_seq TO "${owner}";