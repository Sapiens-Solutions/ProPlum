CREATE OR REPLACE FUNCTION ${target_schema}.f_load_id_exists(p_object_id int8, p_start_date timestamp, p_end_date timestamp)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function checks does active load_id already exists */

DECLARE
    v_location    text := '${target_schema}.f_load_id_exists';
    v_exists      int;
    v_extraction_type text;
    v_periodicity text;
    c_new_status  int  := 1;
    c_work_status int  := 2;
    c_maxDate     date := to_date('99991231', 'YYYYMMDD');
    v_sql         text;
begin
	
	perform ${target_schema}.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'START check load_id exists for object ' || p_object_id, 
       p_location    := v_location); 
    --	Get template_type for object_id
    select extraction_type, periodicity::text
    into v_extraction_type, v_periodicity
    from ${target_schema}.objects ob
    where ob.object_id = p_object_id;

    v_sql := 'select count(0) from ${target_schema}.load_info where object_id = ' || p_object_id::text || ' and load_status in (' ||
             c_new_status::text || ', ' || c_work_status::text || ') and ' ||
             case
                 when v_extraction_type = 'PARTITION' then 'extraction_from <= ''' || p_start_date::text || '''::date and extraction_to >= ''' ||
                                             p_end_date::text || '''::date'
                 when v_extraction_type = 'DELTA' and v_periodicity  ~ '^.*(mon)|(year)$'  then 'extraction_from <= ''' || p_start_date::text || '''::date and extraction_to >= ''' ||
                                             p_end_date::text || '''::date'
                 else 'extraction_from <= ''' || p_start_date::text || '''::timestamp and extraction_to >= ''' ||
                      p_end_date::text ||
                      '''::timestamp and (extraction_to < current_date + ''2 days''::interval or extraction_to = ''' ||
                      c_maxDate::text || '''::date)' end;
    perform ${target_schema}.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'Execute check sql: ' || v_sql, 
       p_location    := v_location);  --log function call
    execute v_sql into v_exists;
   	perform ${target_schema}.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'END check load_id exists for object ' || p_object_id||', load_id '||case when v_exists > 0 then 'exists' else 'not exists' end, 
       p_location    := v_location); 
    RETURN v_exists > 0;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_load_id_exists(int8, timestamp, timestamp) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_load_id_exists(int8, timestamp, timestamp) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_load_id_exists(int8, timestamp, timestamp) TO "${owner}";
