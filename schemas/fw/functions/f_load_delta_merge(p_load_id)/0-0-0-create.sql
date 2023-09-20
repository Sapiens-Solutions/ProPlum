CREATE OR REPLACE FUNCTION fw.f_load_delta_merge(p_load_id int8)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function merges one table to another by switching default partition*/
DECLARE
    v_location          text := 'fw.f_load_delta_merge';
    v_cnt               int8; 
    v_where             text;
    v_sql               text;
    v_merge_key_arr     _text;
    v_table_to_name     text;
    v_delta_table_name  text;
    v_buffer_table_name text;
    v_buf_prefix        text;
    v_object_id         int8;
begin
    PERFORM fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'START Merge data for load_id = '||p_load_id, 
       p_location    := v_location,
       p_load_id     := p_load_id);
	v_sql := 'select ob.object_id, ob.object_name, ob.merge_key
              from fw.load_info li, fw.objects ob where li.object_id = ob.object_id and li.load_id = ' ||
              p_load_id::text;
    execute v_sql into v_object_id, v_table_to_name, v_merge_key_arr;
    
    if v_merge_key_arr is null THEN
        PERFORM fw.f_write_log(
           p_log_type    := 'ERROR', 
           p_log_message := 'No merge key is defined for table '||v_table_to_name, 
           p_location    := v_location);
        RAISE EXCEPTION 'No merge key is defined for table % ', v_table_to_name;
    end if;
    v_where = coalesce(fw.f_get_where_clause(p_object_id := v_object_id),'1=1');
    v_buf_prefix = fw.f_get_constant('c_buf_table_prefix');
    v_delta_table_name  =  fw.f_get_delta_table_name(p_load_id := p_load_id);
    v_buffer_table_name =  fw.f_create_tmp_table(
      p_table_name  := v_table_to_name, 
      p_prefix_name := v_buf_prefix, 
      p_drop_table  := true);
     
    v_cnt = fw.f_merge_tables(
      p_table_from_name := v_delta_table_name,
      p_table_to_name   := v_table_to_name,
      p_where     := v_where,
      p_merge_key := v_merge_key_arr,
      p_trg_table := v_buffer_table_name);
    if v_cnt = 0 then 
      PERFORM fw.f_write_log(
         p_log_type    := 'SERVICE', 
         p_log_message := 'There are no new data for interval: '||v_start_bdate||' - '||v_iterDate||', skip switch partition', 
         p_location    := v_location,
         p_load_id     := p_load_id); --log function call
    else 
      -- switch default partition in target table
      perform fw.f_switch_def_partition(
         p_table_from_name := v_buffer_table_name, 
         p_table_to_name   := v_table_to_name);
    end if;
    PERFORM fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'END Merge data for load_id = '||p_load_id||', '||v_cnt||' records loaded', 
       p_location    := v_location);
    return v_cnt; 
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_load_delta_merge(int8) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_load_delta_merge(int8) TO public;
GRANT ALL ON FUNCTION fw.f_load_delta_merge(int8) TO "admin";
