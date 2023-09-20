CREATE OR REPLACE FUNCTION fw.f_update_load_info(p_load_id int8, p_field_name text, p_value text)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function update field of load_info with value */
DECLARE
  v_location text := 'fw.f_update_load_info';
  v_sql text; 
  v_datatype text;
  v_res text;
  v_server text;
BEGIN

  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START set fw.load_info.'||p_field_name||' = '||coalesce(p_value,'{empty}')||' for load_id = '||p_load_id, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  v_server = fw.f_get_constant('c_log_fdw_server');
  v_sql = 'select data_type from information_schema.columns where table_schema ||''.''||table_name = ''fw.load_info'' and column_name = '''||p_field_name||'''';
  execute v_sql into v_datatype;
  if v_datatype is null then
     PERFORM fw.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'No field with name '||p_field_name||' in table fw.load_info', 
        p_location    := v_location,
        p_load_id     := p_load_id);
     raise exception 'No field with name % in table fw.load_info',p_field_name;
  end if;
  v_sql = 'UPDATE fw.load_info set '||p_field_name||'='''||p_value||'''::'||v_datatype||',updated_dttm = '''||current_timestamp||''' where load_id = '||p_load_id;
  perform fw.f_write_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'UPDATE sql is: '||v_sql, 
   p_location    := v_location,
   p_load_id     := p_load_id); --log function call
  v_res := dblink(v_server,v_sql); 
  --execute v_sql;
  perform fw.f_write_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'END set fw.load_info.'||p_field_name||' = '||p_value||' for load_id = '||p_load_id, 
   p_location    := v_location,
   p_load_id     := p_load_id); --log function call
  exception when others then 
     raise notice 'ERROR %, while set fw.load_info.% = % for load_id = %',sqlerrm,p_field_name,coalesce(p_value,'{empty}'),p_load_id;
     PERFORM fw.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Set fw.load_info.'||p_field_name||' = '||p_value||' for load_id = '||p_load_id||' finished with error: '||SQLERRM, 
        p_location    := v_location,
        p_load_id     := p_load_id);
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_update_load_info(int8, text, text) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_update_load_info(int8, text, text) TO public;
GRANT ALL ON FUNCTION fw.f_update_load_info(int8, text, text) TO "admin";
