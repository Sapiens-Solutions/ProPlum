CREATE OR REPLACE FUNCTION fw.f_get_load_locks(p_object_name text DEFAULT NULL::text)
	RETURNS SETOF locks
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function get locks on table*/
DECLARE
  v_location text := 'fw.f_get_load_locks';
  v_server text;
BEGIN
   v_server = fw.f_get_constant('c_log_fdw_server');
   perform fw.f_write_log( 
     p_log_type    := 'SERVICE', 
     p_log_message := 'Get locks on object: '||coalesce(p_object_name,'{all}'),
     p_location    := v_location); --log function call
   return query
    select * from dblink(v_server, 'select * from fw.locks where 1=1 '||coalesce(' and object_name = '''||p_object_name||'''','')) as t1 (load_id int8, pid int4, lock_type text, object_name text, lock_timestamp timestamp, lock_user text);
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_get_load_locks(text) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_get_load_locks(text) TO public;
GRANT ALL ON FUNCTION fw.f_get_load_locks(text) TO "admin";
