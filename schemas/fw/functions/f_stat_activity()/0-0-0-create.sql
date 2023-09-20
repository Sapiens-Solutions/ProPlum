CREATE OR REPLACE FUNCTION fw.f_stat_activity()
	RETURNS SETOF pg_stat_activity
	LANGUAGE sql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	 SELECT * FROM pg_catalog.pg_stat_activity; 


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_stat_activity() OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_stat_activity() TO public;
GRANT ALL ON FUNCTION fw.f_stat_activity() TO "admin";
