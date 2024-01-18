CREATE OR REPLACE FUNCTION ${target_schema}.f_extract_data(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function loads data into stage table*/
DECLARE
    v_location            text := '${target_schema}.f_extract_data';
    v_extraction_type     text;
    v_extraction_to       timestamp;
    v_extraction_from     timestamp;
    v_tmp_table_name      text;
    v_ext_table_name      text;
    v_delta_field         text;
    v_error               text;
    v_sql                 text;
    v_where               text;
    v_res                 bool;
    v_cnt                 int8;
BEGIN
	perform ${target_schema}.f_write_log(p_log_type := 'SERVICE', 
       p_log_message := 'START extract data for load_id = '||p_load_id, 
       p_location    := v_location,
       p_load_id     := p_load_id); --log function call
   	--set load_id for session
    perform ${target_schema}.f_set_session_param(
       p_param_name  := '${target_schema}.load_id', 
       p_param_value := p_load_id::text);
    perform ${target_schema}.f_set_load_id_in_process(
       p_load_id := p_load_id);
    -- Get table load type
    v_sql := 'select coalesce(li.extraction_type, ob.extraction_type), 
              case coalesce(li.extraction_type, ob.extraction_type) 
                when ''DELTA'' then ob.delta_field
                when ''PARTITION'' then ob.bdate_field
                else null::text
              end,
              li.extraction_to,
			  li.extraction_from
              from ${target_schema}.load_info li, ${target_schema}.objects ob where li.object_id = ob.object_id and li.load_id = ' ||
             p_load_id::text;
    execute v_sql into v_extraction_type, v_delta_field, v_extraction_to, v_extraction_from;
    v_tmp_table_name = ${target_schema}.f_get_delta_table_name(p_load_id := p_load_id);
    v_ext_table_name = ${target_schema}.f_get_ext_table_name(p_load_id := p_load_id);
    v_where = ${target_schema}.f_get_extract_where_cond(p_load_id := p_load_id);
    -- process where clause
    IF v_extraction_type in (select distinct extraction_type from ${target_schema}.d_extraction_type) 
     then
        perform  ${target_schema}.f_truncate_table(p_table_name := v_tmp_table_name);
        v_sql := ${target_schema}.f_get_load_expression(p_load_id := p_load_id)||' where '||v_where;      
        v_cnt =  ${target_schema}.f_insert_table_sql(
           p_table_to := v_tmp_table_name,
           p_sql      := v_sql); --load from ext to stage (delta) table
        if v_cnt is not null then
         v_res = true;
		 if v_cnt = 0 then -- in case of empty delta
           perform ${target_schema}.f_update_load_info(
             p_load_id    := p_load_id, 
             p_field_name := 'extraction_to', 
             p_value      := v_extraction_from::text);
         else
           perform ${target_schema}.f_update_load_info(
             p_load_id    := p_load_id, 
             p_field_name := 'extraction_to', 
             p_value      := coalesce(${target_schema}.f_get_max_value(v_tmp_table_name,v_delta_field),v_extraction_to::text));
         end if;
        else 
         v_res = false;
        end if;
     ELSE
        v_error := 'Unable to process extraction type '||v_extraction_type;
        perform ${target_schema}.f_write_log(
           p_log_type := 'ERROR', 
           p_log_message := 'Error while extraction: ' || v_error, 
           p_location    := v_location,
           p_load_id     := p_load_id); --log function call
        RAISE NOTICE '%',v_error;
        perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);
        v_res = false;
    END IF;  
    if v_res is true then
      -- Log Success
      perform ${target_schema}.f_write_log(
         p_log_type := 'SERVICE', 
         p_log_message := 'END extract data for load_id = '||p_load_id||', '||v_cnt||' rows extracted',
         p_location    := v_location,
         p_load_id     := p_load_id); --log function call
      return v_res;
    else 
      PERFORM ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);
      -- Log errors
      perform ${target_schema}.f_write_log(
         p_log_type := 'SERVICE', 
         p_log_message := 'END extract data for load_id = '||p_load_id||' finished with error', 
         p_location    := v_location,
         p_load_id     := p_load_id); --log function call
      return false;
     end if;
    exception when others then 
     raise notice 'ERROR while extract data for load_id = %: %',p_load_id,SQLERRM;
     PERFORM ${target_schema}.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Extract data for load_id '||p_load_id||' finished with error: '||SQLERRM, 
        p_location    := v_location,
        p_load_id     := p_load_id);
     perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
     return false;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_extract_data(int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_extract_data(int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_extract_data(int8) TO "${owner}";
