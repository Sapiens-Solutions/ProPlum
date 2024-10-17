CREATE OR REPLACE FUNCTION ${target_schema}.f_post_extract_odata(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
    /*New function*/
DECLARE
    v_location            text := '${target_schema}.f_post_extract_odata';
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
		select oqh.rows_count, oqh.delta_field, oqh.extraction_to from ${target_schema}.odata_query_helper oqh where oqh.load_id = p_load_id  
		into v_cnt, v_delta_field, v_extraction_to ;
		if v_cnt is null or v_cnt < 0 
		then
        	perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);
	    	perform ${target_schema}.f_write_log(
	        	 p_log_type := 'SERVICE', 
	         	p_log_message := 'END extract data for load_id = '||p_load_id||' finished with error', 
	         	p_location    := v_location,
	         	p_load_id     := p_load_id); --log function call
	       return false;
        else 
           perform ${target_schema}.f_update_load_info(
             p_load_id    := p_load_id, 
             p_field_name := 'extraction_to', 
             p_value      := coalesce(${target_schema}.f_get_max_value(v_tmp_table_name,v_delta_field),v_extraction_to::text));
	 	   perform ${target_schema}.f_write_log(
	         p_log_type := 'SERVICE', 
	         p_log_message := 'END extract data for load_id = '||p_load_id||', '||v_cnt||' rows extracted',
	         p_location    := v_location,
	         p_load_id     := p_load_id); --log function call
	       return true;
      -- Log errors
     	end if;
    exception when others then 
     raise notice 'ERROR while extract data for load_id = %: %',p_load_id,SQLERRM;
     perform ${target_schema}.f_write_log(
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

ALTER FUNCTION ${target_schema}.f_post_extract_odata(int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_post_extract_odata(int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_post_extract_odata(int8) TO "${owner}";
