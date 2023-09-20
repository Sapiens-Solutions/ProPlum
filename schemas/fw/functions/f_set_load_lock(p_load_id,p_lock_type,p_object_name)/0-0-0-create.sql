CREATE OR REPLACE FUNCTION fw.f_set_load_lock(p_load_id int8, p_lock_type text, p_object_name text)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function set lock on table*/
DECLARE
  v_location text := 'fw.f_set_load_lock';
  v_res text;
  v_pid int4;
  v_server text;
  v_sql text;
  v_cnt int4; 
BEGIN
   --check lock already exist
   select count(1) from fw.f_get_load_locks(p_object_name := p_object_name) into v_cnt;
   if v_cnt > 0 then 
    perform fw.f_write_log(
      p_log_type    := 'SERVICE', 
      p_log_message := 'Cant set '||p_lock_type ||' lock on object: '||p_object_name||' because it already locked', 
      p_location    := v_location,
      p_load_id     := p_load_id); --log function call
    return false;
   end if;
   v_server = fw.f_get_constant('c_log_fdw_server');
   v_pid = pg_backend_pid();  
   v_sql = 'INSERT INTO fw.locks(load_id, pid, lock_type, object_name, lock_timestamp,lock_user) 
            VALUES ( '||coalesce(p_load_id, 0)||', '||v_pid||', '||coalesce(''''||p_lock_type||'''','''empty''')||', '''||p_object_name||''',current_timestamp,current_user);';
   perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Set '||p_lock_type ||' lock on object: '||p_object_name, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
   v_res =  dblink(v_server,v_sql); 
   raise notice 'Set lock result: %',v_res;
   return true;
   exception when others then 
    perform fw.f_write_log(
      p_log_type    := 'ERROR', 
      p_log_message := 'Set '||p_lock_type ||' lock on object: '||p_object_name||' finished with error: '||sqlerrm , 
      p_location    := v_location,
      p_load_id     := p_load_id); --log function call
    return false;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_set_load_lock(int8, text, text) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_set_load_lock(int8, text, text) TO public;
GRANT ALL ON FUNCTION fw.f_set_load_lock(int8, text, text) TO "admin";
