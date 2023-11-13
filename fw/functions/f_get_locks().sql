CREATE OR REPLACE FUNCTION ${target_schema}.f_get_locks()
	RETURNS SETOF pg_locks
	LANGUAGE sql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	 SELECT * FROM pg_catalog.pg_locks; 


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_get_locks() OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_get_locks() TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_get_locks() TO "${owner}";
