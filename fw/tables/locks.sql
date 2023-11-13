CREATE TABLE ${target_schema}.locks (
	load_id int8 NULL,
	pid int4 NULL,
	lock_type text NULL,
	object_name text NULL,
	lock_timestamp timestamp NULL DEFAULT now(),
	lock_user text NULL DEFAULT "current_user"()
)
DISTRIBUTED BY (load_id);

-- Permissions

ALTER TABLE ${target_schema}.locks OWNER TO "${owner}";
GRANT ALL ON TABLE ${target_schema}.locks TO "${owner}";