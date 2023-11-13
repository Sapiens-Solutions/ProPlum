CREATE OR REPLACE FUNCTION ${target_schema}.f_get_connection_string(p_load_id int8)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
	/*get connection string for external table*/
declare
    v_location text := '${target_schema}.f_get_connection_string';
    v_extraction_type text;
    v_load_method text;
    v_conn_str text;
    v_sql_conn text;
    v_part_string text;
    v_start_date timestamp;
    v_end_date timestamp;
    v_delta_fld text;
    v_bdate_fld text;
    v_error text;
begin
	--Function returns connection string for external table, also checks settings for external tables in table ${target_schema}.ext_tables_params
    perform ${target_schema}.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'START Get connection string for load_id = ' || p_load_id, 
       p_location    := v_location,
       p_load_id     := p_load_id);
    select li.extraction_type, coalesce(etp.load_method,ob.load_method), coalesce(etp.connection_string, ob.connect_string),li.extraction_from, li.extraction_to, ob.delta_field, ob.bdate_field
     from  ${target_schema}.load_info li 
      join ${target_schema}.objects ob on ob.object_id = li.object_id
      left join ${target_schema}.ext_tables_params etp on ob.object_id = etp.object_id and etp."active" is true
     where li.load_id = p_load_id
    into v_extraction_type, v_load_method,v_conn_str,v_start_date,v_end_date,v_delta_fld,v_bdate_fld;
    if coalesce(v_conn_str,'') = '' then
     perform ${target_schema}.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'Connection string for load_id = ' || p_load_id||' is empty',
       p_location    := v_location,
       p_load_id     := p_load_id);
     RAISE notice 'Connection string for load_id = % is empty',p_load_id;
     return null::text;
    end if;

    v_conn_str =  replace(replace(replace(replace(replace(replace(replace(replace(replace(v_conn_str,
                  '$load_id',coalesce(p_load_id::text,''::text)),
                  '$current_date',to_char(current_date,'YYYY-MM-DD')),
                  '$now',to_char(now(),'YYYY-MM-DD')),
                  '$extraction_from',to_char(v_start_date,'YYYY-MM-DD')::text),
                  '$extraction_to',to_char(v_end_date,'YYYY-MM-DD')::text),
                  '$load_from',to_char(v_start_date,'YYYY-MM-DD')::text),
                  '$load_to',to_char(v_end_date,'YYYY-MM-DD')::text),
                  '$delta_field',coalesce(v_delta_fld,'')::text),
                  '$bdate_field',coalesce(v_bdate_fld,'')::text);
    RAISE notice 'v_conn_str: %',coalesce(v_conn_str,'empty');
    case v_load_method 
     when 'gpfdist' then 
      v_sql_conn := v_conn_str;
     when 'dblink' then 
      v_sql_conn := v_conn_str;
     when 'pxf' then
      v_part_string = coalesce(${target_schema}.f_get_pxf_partition(p_load_id := p_load_id),'');
      v_sql_conn :=
      'LOCATION (''pxf://'||v_conn_str||v_part_string||''') ON ALL FORMAT ''CUSTOM'' ( FORMATTER=''pxfwritable_import'' )
       ENCODING ''UTF8''';
     when 'python' then 
    -- no need to create external table for python load method
      perform ${target_schema}.f_write_log(
         p_log_type    := 'SERVICE', 
         p_log_message := 'END Creating external table for table '||v_full_table_name|| '. No need to create external table for python load method', 
         p_location    := v_location); --log function call
      return ''::text;
     else
      v_error := 'Unknown load method '|| v_load_method;
      perform ${target_schema}.f_write_log(
         p_log_type    := 'ERROR', 
         p_log_message := v_error, 
         p_location    := v_location); --log function call
      RAISE EXCEPTION '%',v_error;
     end case;
    perform ${target_schema}.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'END Get connection string for load_id = ' || p_load_id||', connect string: '||v_sql_conn, 
        p_location    := v_location,
        p_load_id     := p_load_id);
    return v_sql_conn;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_get_connection_string(int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_get_connection_string(int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_get_connection_string(int8) TO "${owner}";
