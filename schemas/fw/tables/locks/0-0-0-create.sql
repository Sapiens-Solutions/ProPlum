-- fw.locks definition

-- Drop table

-- DROP TABLE fw.locks;

CREATE TABLE fw.locks (
	load_id int8 NULL,
	pid int4 NULL,
	lock_type text NULL,
	object_name text NULL,
	lock_timestamp timestamp NULL DEFAULT now(),
	lock_user text NULL DEFAULT "current_user"()
)
DISTRIBUTED BY (load_id);

-- Permissions

ALTER TABLE fw.locks OWNER TO "d.ismailov";
GRANT ALL ON TABLE fw.locks TO "d.ismailov";