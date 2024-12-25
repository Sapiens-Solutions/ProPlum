CREATE OR REPLACE FUNCTION ${target_schema}.f_prepare_extract_odata(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
    /*New function*/
DECLARE
    v_location            text := '${target_schema}.f_prepare_extract_odata';
    v_extraction_type     text;
    v_extraction_to       timestamp;
    v_extraction_from     timestamp;
    v_tmp_table_name      text;
    v_ext_table_name      text;
    v_delta_field         text;
    v_error               text;
    v_sql                 text;
    v_where               text;
    v_res                 text;
    v_cnt                 int8;
    v_server              text;
BEGIN
	perform ${target_schema}.f_write_log(p_log_type := 'SERVICE', 
       p_log_message := 'START prepare extract odata data for load_id = '||p_load_id, 
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
                else coalesce(ob.delta_field,ob.bdate_field,null)::text
              end,
              li.extraction_to,
              li.extraction_from
              from ${target_schema}.load_info li, ${target_schema}.objects ob where li.object_id = ob.object_id and li.load_id = ' ||
             p_load_id::text;
    execute v_sql into v_extraction_type, v_delta_field, v_extraction_to, v_extraction_from;
    v_server = lower(${target_schema}.f_get_constant('c_log_fdw_server'));
    v_tmp_table_name = ${target_schema}.f_get_delta_table_name(p_load_id := p_load_id);
    v_ext_table_name = ${target_schema}.f_get_ext_table_name(p_load_id := p_load_id);
    v_where = ${target_schema}.f_get_extract_where_cond(p_load_id := p_load_id);
    -- process where clause
    IF v_extraction_type in (select distinct extraction_type from ${target_schema}.d_extraction_type) 
     then
        perform  ${target_schema}.f_truncate_table(p_table_name := v_tmp_table_name);
        v_sql := replace(${target_schema}.f_get_load_expression(p_load_id := p_load_id)||' where '||v_where,'''','''''');
        v_sql := ${target_schema}.f_replace_variables(p_load_id,v_sql);
        v_sql := 'INSERT INTO ${target_schema}.odata_query_helper 
                   VALUES ( '||p_load_id||' , '''||v_sql||''', '''||v_tmp_table_name||''', '||0||', '||coalesce(''''||v_delta_field||'''','NULL')||', '''||v_extraction_to||''');';
        --dblink for fdw server
        raise notice 'insert sql into query helper: = %',v_sql;
        v_res := dblink(v_server,v_sql);
        --insert into ${target_schema}.odata_query_helper values (p_load_id, v_sql, v_tmp_table_name, 0, v_delta_field, v_extraction_to);
     return true;
     ELSE
        v_error := 'Unable to process extraction type '||v_extraction_type;
        perform ${target_schema}.f_write_log(
           p_log_type := 'ERROR', 
           p_log_message := 'Error while extraction: ' || v_error, 
           p_location    := v_location,
           p_load_id     := p_load_id); --log function call
        RAISE NOTICE '%',v_error;
        perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);
        return false;
    END IF;  

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

ALTER FUNCTION ${target_schema}.f_prepare_extract_odata(int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_prepare_extract_odata(int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_prepare_extract_odata(int8) TO "${owner}";
