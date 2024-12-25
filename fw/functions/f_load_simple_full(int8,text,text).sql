CREATE OR REPLACE FUNCTION ${target_schema}.f_load_simple_full(p_load_id int8, p_src_table text, p_trg_table text DEFAULT NULL::text)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function starts full load function */
DECLARE
  v_location      text := '${target_schema}.f_load_simple_full';
  v_cnt           int8;
  v_src_table     text;
  v_trg_table     text;
BEGIN
 -- function load data from source table into target
 
 perform ${target_schema}.f_set_session_param(
    p_param_name := '${target_schema}.load_id', 
    p_param_value := p_load_id::text);
 select ob.object_name
   from ${target_schema}.objects ob  inner join 
	    ${target_schema}.load_info li 
	 on ob.object_id = li.object_id    
   where li.load_id  = p_load_id
   into v_trg_table; -- get object_id and target table
   
  v_src_table  = ${target_schema}.f_unify_name(p_name := p_src_table);
  v_trg_table  = coalesce(${target_schema}.f_unify_name(p_name := p_trg_table),v_trg_table);
  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START simple full load from table '||v_src_table||' into table '||v_trg_table||' with load_id = '||p_load_id, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call

  PERFORM ${target_schema}.f_wait_locks(
     p_table_name      := v_trg_table, 
     p_repeat_interval := 60,
     p_repeat_count    := 60); --wait for no locks on main table every 1 minute 60 times
	 
  --add partitions if needed 
  perform ${target_schema}.f_create_date_partitions(
     p_table_name      := v_trg_table, 
     p_partition_value := current_timestamp::timestamp);
	 
  v_cnt = ${target_schema}.f_load_full(
     p_trg_table := v_trg_table,
     p_src_table := v_src_table); --switch tmp and main table
  if v_cnt is null then
    raise notice 'ERROR Load object with load_id = %',p_load_id;
    return false;
  end if;
  perform ${target_schema}.f_update_load_info( 
     p_load_id    := p_load_id,
     p_field_name := 'row_cnt',
     p_value      := v_cnt::text);-- update row_cnt in load_info
  perform ${target_schema}.f_write_log(
    p_log_type    := 'SERVICE', 
    p_log_message := 'END simple full load from table '||p_src_table||' into table '||p_trg_table||' with load_id = '||p_load_id||', '||v_cnt||' rows inserted', 
    p_location    := v_location,
    p_load_id     := p_load_id); --log function call
  return true;
  exception when others then 
    raise notice 'ERROR Load object with load_id = %: %',p_load_id,SQLERRM;
    PERFORM ${target_schema}.f_write_log(
      p_log_type    := 'ERROR', 
      p_log_message := 'Load object with load_id = '||p_load_id||' finished with error: '||SQLERRM, 
      p_location    := v_location,
      p_load_id     := p_load_id);
    perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
   return false;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_load_simple_full(int8, text, text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_load_simple_full(int8, text, text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_load_simple_full(int8, text, text) TO "${owner}";
