CREATE OR REPLACE FUNCTION ${target_schema}.f_gen_group_load_id(p_load_group text, p_start_date timestamp DEFAULT NULL::timestamp without time zone, p_end_date timestamp DEFAULT NULL::timestamp without time zone, p_load_type text DEFAULT NULL::text, p_load_template text DEFAULT NULL::text, p_load_interval interval DEFAULT NULL::interval, p_start_update timestamp DEFAULT NULL::timestamp without time zone, p_end_update timestamp DEFAULT NULL::timestamp without time zone)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/    
/*Function generates load_id for objects in load group*/
DECLARE
    v_location text := '${target_schema}.f_gen_group_load_id';
    v_object_id int8;
    rec record;
    v_sql text;
    v_load_id int8;
    v_load_type text;
BEGIN	

    perform ${target_schema}.f_write_log(
       p_log_type := 'SERVICE', 
       p_log_message := 'START Generate load_id for load_group '||p_load_group, 
       p_location    := v_location); --log function call

  FOR rec IN
      SELECT object_id, load_type, load_template, load_interval FROM ${target_schema}.objects o WHERE o.load_group = p_load_group and o.active
  loop
      v_sql = 'select ${target_schema}.f_gen_load_id(
                  p_object_id := '||rec.object_id||',
                  p_start_date := '||coalesce(''''||p_start_date||'''::timestamp','null::timestamp')||',
                  p_end_date := '||coalesce(''''||p_end_date||'''::timestamp','null::timestamp')||',
                  p_load_type := '||coalesce(''''||rec.load_type||'''','null::text')||',
                  p_load_template := '||coalesce(''''||rec.load_template||'''','null::text')||',
                  p_load_interval := '||coalesce(''''||p_load_interval||'''::interval','null::interval')||');';            
      execute v_sql into v_load_id;
      perform ${target_schema}.f_write_log(
       p_log_type := 'SERVICE', 
       p_log_message := 'Load_id for object '||rec.object_id||' is: '||v_load_id, 
       p_location    := v_location); --log function call
  END LOOP; 
    perform ${target_schema}.f_write_log(
       p_log_type := 'SERVICE', 
       p_log_message := 'Finish Generate load_id for load_group '||p_load_group, 
       p_location    := v_location); --log function call
   return true; 
  exception when others then 
     raise notice 'Function % finished with error: %',v_location,sqlerrm;
     PERFORM ${target_schema}.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Function '||v_location||' finished with error: '||SQLERRM, 
        p_location    := v_location);
     return false;  
END;
$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_gen_group_load_id(text, timestamp, timestamp, text, text, interval, timestamp, timestamp) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_gen_group_load_id(text, timestamp, timestamp, text, text, interval, timestamp, timestamp) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_gen_group_load_id(text, timestamp, timestamp, text, text, interval, timestamp, timestamp) TO "${owner}";
