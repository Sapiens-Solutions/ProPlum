CREATE OR REPLACE FUNCTION ${target_schema}.f_set_instance_id_success(instance_id int8)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2024*/
/*Set in process status to instance_id*/
declare
    c_success_status int  := 3;
    v_server       text;
    v_sql          text;
begin
    v_server = ${target_schema}.f_get_constant('c_log_fdw_server');
	v_sql = 'update ${target_schema}.chains_info set status = ' || c_success_status::text || ', chain_finish = '''||current_timestamp||''' where instance_id = ' || instance_id::text;
    perform dblink(v_server,v_sql);
    perform ${target_schema}.f_write_chain_log(
       p_log_type := 'SERVICE', 
       p_log_message := 'Set in success instance_id = '||instance_id, 
       p_instance_id := instance_id); --log function call
end;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_set_instance_id_success(int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_set_instance_id_success(int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_set_instance_id_success(int8) TO "${owner}";
