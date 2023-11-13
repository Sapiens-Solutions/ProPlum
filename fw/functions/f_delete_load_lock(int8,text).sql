CREATE OR REPLACE FUNCTION ${target_schema}.f_delete_load_lock(p_load_id int8 DEFAULT NULL::bigint, p_object_name text DEFAULT NULL::text)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function delete locks on table*/
DECLARE
  v_location text := '${target_schema}.f_delete_load_lock';
  v_server text;
  v_res text;
  v_sql text;
BEGIN
   v_server = ${target_schema}.f_get_constant('c_log_fdw_server');
   v_sql = 'DELETE from ${target_schema}.locks where 1=1 '||coalesce('and load_id = '||p_load_id,'')||coalesce(' and object_name = '''||p_object_name||'''','');
   raise notice 'Delete lock sql: %',v_sql;
   perform ${target_schema}.f_write_log( 
     p_log_type    := 'SERVICE', 
     p_log_message := 'Delete locks on object '||coalesce(p_object_name,'{all}')||' with load_id = '||coalesce(p_load_id::text,'{all}'), 
     p_location    := v_location); --log function call
   v_res =  dblink(v_server,v_sql); 
   return true;
   exception when others then 
    perform ${target_schema}.f_write_log(
      p_log_type    := 'ERROR', 
      p_log_message := 'Delete lock on object: '||coalesce(p_object_name,'{all}')||', load_id = '||p_load_id||' finished with error: '||sqlerrm , 
      p_location    := v_location); --log function call
    return false;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_delete_load_lock(int8, text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_delete_load_lock(int8, text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_delete_load_lock(int8, text) TO "${owner}";
