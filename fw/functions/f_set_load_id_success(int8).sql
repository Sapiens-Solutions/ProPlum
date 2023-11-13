CREATE OR REPLACE FUNCTION ${target_schema}.f_set_load_id_success(p_load_id int8)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Set success status to load_id*/
declare
    v_location             text := '${target_schema}.f_set_load_id_success';
    c_success_status       int  := 3;
    v_server               text;
    v_sql                  text;
begin
	v_server = ${target_schema}.f_get_constant('c_log_fdw_server');
    v_sql = 'update ${target_schema}.load_info set load_status = ' || c_success_status::text || ', updated_dttm = current_timestamp where load_id = ' || p_load_id::text;
    perform dblink(v_server,v_sql);
    perform ${target_schema}.f_write_log(
       p_log_type := 'SERVICE', 
       p_log_message := 'Successfully finished processing load_id = '||p_load_id, 
       p_location    := v_location,
       p_load_id     := p_load_id); --log function call
end;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_set_load_id_success(int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_set_load_id_success(int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_set_load_id_success(int8) TO "${owner}";
