CREATE OR REPLACE FUNCTION ${target_schema}.f_load_simple_upsert(p_load_id int8, p_src_table text, p_trg_table text DEFAULT NULL::text, p_delete_duplicates bool DEFAULT false, p_where text DEFAULT NULL::text)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function load data from stage into target by delete-insert */
DECLARE
  v_location      text := '${target_schema}.f_load_simple_upsert';
  v_object_id     text;
  v_cnt           int8;
  v_src_table     text;
  v_trg_table     text;
  v_where         text;
  v_end_date      timestamp;
BEGIN
 -- function load upsert data from source table into target
 perform ${target_schema}.f_set_session_param(
    p_param_name := '${target_schema}.load_id', 
    p_param_value := p_load_id::text);
 select ob.object_id, ob.object_name
   from ${target_schema}.objects ob  inner join 
	    ${target_schema}.load_info li 
	 on ob.object_id = li.object_id    
   where li.load_id  = p_load_id
   into v_object_id, v_trg_table; -- get object_id and target table
  v_src_table  = ${target_schema}.f_unify_name(p_name := p_src_table);
  v_trg_table  = coalesce(${target_schema}.f_unify_name(p_name := p_trg_table),v_trg_table);
  v_where = coalesce(p_where, '1=1');
 perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START simple upsert load from table '||v_src_table||' into table '||v_trg_table||' with load_id = '||p_load_id|| ' and condition: '||v_where, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call

 v_cnt = ${target_schema}.f_upsert_table(
    p_table_from_name := v_src_table,
    p_table_to_name   := v_trg_table,
    p_load_id         := p_load_id,
    p_delete_duplicates := p_delete_duplicates,
    p_where           := v_where);
 v_end_date = ${target_schema}.f_get_max_value(v_tmp_table_name,v_delta_fld)::timestamp;
 perform ${target_schema}.f_update_load_info(
    p_load_id    := p_load_id,
    p_field_name := 'load_to',
    p_value      := v_end_date::text);
   if v_cnt is null then
  	return false;
   end if;
 perform ${target_schema}.f_update_load_info(
    p_load_id    := p_load_id,
    p_field_name := 'row_cnt',
    p_value      := v_cnt::text);
 perform ${target_schema}.f_write_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'END simple upsert load from table '||p_src_table||' into table '||p_trg_table||' with load_id = '||p_load_id||', '||v_cnt||' rows inserted', 
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

ALTER FUNCTION ${target_schema}.f_load_simple_upsert(int8, text, text, bool, text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_load_simple_upsert(int8, text, text, bool, text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_load_simple_upsert(int8, text, text, bool, text) TO "${owner}";
