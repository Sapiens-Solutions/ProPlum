CREATE OR REPLACE FUNCTION ${target_schema}.f_update_chains_info(p_instance_id int8, p_field_name text, p_value text)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2024*/
/*Function update field of chains_info with value */
DECLARE
  v_sql text; 
  v_datatype text;
  v_res text;
  v_server text;
BEGIN
  perform ${target_schema}.f_write_chain_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START set ${target_schema}.chains_info'||p_field_name||' = '||coalesce(p_value,'{empty}')||' for instance_id = '||p_instance_id, 
     p_instance_id := p_instance_id); --log function call
  v_server = ${target_schema}.f_get_constant('c_log_fdw_server');
  v_sql = 'select data_type from information_schema.columns where table_schema ||''.''||table_name = ''${target_schema}.chains_info'' and column_name = '''||p_field_name||'''';
  execute v_sql into v_datatype;
  if v_datatype is null then
     PERFORM ${target_schema}.f_write_chain_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'No field with name '||p_field_name||' in table ${target_schema}.chains_info', 
        p_instance_id := p_instance_id);
     raise exception 'No field with name % in table ${target_schema}.chains_info',p_field_name;
  end if;
  v_sql = 'UPDATE ${target_schema}.chains_info set '||p_field_name||'='''||p_value||'''::'||v_datatype||' where instance_id = '||p_instance_id;
  perform ${target_schema}.f_write_chain_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'UPDATE sql is: '||v_sql, 
   p_instance_id := p_instance_id); --log function call
  v_res := dblink(v_server,v_sql); 
  --execute v_sql;
  perform ${target_schema}.f_write_chain_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'END set ${target_schema}.chains_info.'||p_field_name||' = '||p_value||' for instance_id = '||p_instance_id, 
   p_instance_id := p_instance_id); --log function call
  exception when others then 
     raise notice 'ERROR %, while set ${target_schema}.chains_info.% = % for instance_id = %',sqlerrm,p_field_name,coalesce(p_value,'{empty}'),p_instance_id;
     PERFORM ${target_schema}.f_write_chain_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Set ${target_schema}.chains_info.'||p_field_name||' = '||p_value||' for instance_id = '||p_instance_id||' finished with error: '||SQLERRM, 
        p_instance_id     := p_instance_id);
END;
$$
EXECUTE ON ANY;
-- Permissions
ALTER FUNCTION ${target_schema}.f_update_chains_info(int8, text, text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_update_chains_info(int8, text, text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_update_chains_info(int8, text, text) TO "${owner}";
