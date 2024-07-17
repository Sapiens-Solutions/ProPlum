CREATE OR REPLACE FUNCTION ${target_schema}.f_set_instance_id_error(instance_id int8)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2024*/
/*Set error status to instance_id*/
declare
    c_error_status int  := -1;
    v_server       text;
    v_sql          text;
begin
    v_server = ${target_schema}.f_get_constant('c_log_fdw_server');
	v_sql = 'update ${target_schema}.chains_info set status = ' || c_error_status::text || ' where instance_id = ' || p_instance_id::text;
    perform dblink(v_server,v_sql);
    perform ${target_schema}.f_write_chain_log(
       p_log_type := 'SERVICE', 
       p_log_message := 'Set in error instance_id = '||instance_id, 
       p_instance_id     := p_instance_id); --log function call
end;
$$
EXECUTE ON ANY;
-- Permissions
ALTER FUNCTION ${target_schema}.f_set_instance_id_error(int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_set_instance_id_error(int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_set_instance_id_error(int8) TO "${owner}";