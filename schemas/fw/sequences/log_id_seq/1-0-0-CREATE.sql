-- fw.log_id_seq definition

-- DROP SEQUENCE fw.log_id_seq;

CREATE SEQUENCE fw.log_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	NO CYCLE;

-- Permissions

ALTER SEQUENCE fw.log_id_seq OWNER TO "admin";
GRANT ALL ON SEQUENCE fw.log_id_seq TO "admin";