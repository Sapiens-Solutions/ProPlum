CREATE OR REPLACE FUNCTION fw.f_update_table_sql(p_table_to_name text, p_sql text, p_column_list _text, p_load_id int8)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function merges sql to another table by update*/
DECLARE
    v_location      text := 'fw.f_update_table_sql';
    v_table_to_name text;
    v_cnt           int8;
    v_object_id     int8;
    v_merge_key_arr _text;
begin
	
--Update rows from source sql (p_sql) into target table (p_table_to_name) using "merge key" from object settings 

  --Log
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Start update table ' || p_table_to_name ||' from sql '||p_sql,
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
     
  v_table_to_name   = fw.f_unify_name(p_name := p_table_to_name);
  if p_column_list is null then 
   perform fw.f_write_log(
      p_log_type    := 'ERROR',
      p_log_message := 'ERROR while update table ' || v_table_to_name ||', column list is null', 
      p_location    := v_location,
      p_load_id     := p_load_id); --log function call
   return null;
  end if;
 
  select ob.object_id
   from fw.objects ob  inner join 
	    fw.load_info li 
	 on ob.object_id = li.object_id    
   where li.load_id  = p_load_id
   into v_object_id; -- get object_id, target table 

  v_merge_key_arr = fw.f_get_merge_key(p_object_id := v_object_id);
 
  if v_merge_key_arr is null then
    perform fw.f_write_log(
      p_log_type    := 'ERROR', 
      p_log_message := 'ERROR while update table ' ||v_table_to_name||', merge key for object is null', 
      p_location    := v_location,
      p_load_id     := p_load_id); --log function call
    raise notice 'Merge key for object % is null',v_object_id;
    return null;
  end if;

  v_cnt = fw.f_update_table_sql(
     p_table_to_name := v_table_to_name, 
     p_sql           := p_sql, 
     p_column_list   := p_column_list, 
     p_merge_key     := v_merge_key_arr);
    
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'End update table '||v_table_to_name||' from sql: '||p_sql,
     p_location    := v_location); --log function call
  return v_cnt;
 exception when others then 
  raise notice 'ERROR update table % from sql: %, ERROR: %',v_table_to_name,p_sql,SQLERRM;
  PERFORM fw.f_write_log(
     p_log_type    := 'ERROR', 
     p_log_message := 'update table '|| v_table_to_name||' from sql: '||p_sql||' finished with error'', ERROR: '||SQLERRM, 
     p_location    := v_location,
     p_load_id     := p_load_id);
   perform fw.f_set_load_id_error(p_load_id := p_load_id);  
   return null;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_update_table_sql(text, text, _text, int8) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_update_table_sql(text, text, _text, int8) TO public;
GRANT ALL ON FUNCTION fw.f_update_table_sql(text, text, _text, int8) TO "admin";
