CREATE OR REPLACE FUNCTION ${target_schema}.f_get_max_value(p_table_name text, p_field_name text, p_where text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function returns max p_field_name value from p_table_name*/
DECLARE
  v_location text := '${target_schema}.f_get_max_value';
  v_sql text; 
  v_datatype text;
  v_where text;
  v_res text;
BEGIN

  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START find max value from table '||p_table_name||' in field '||p_field_name, 
     p_location    := v_location); --log function call
  --v_sql = 'select data_type from information_schema.columns where table_schema ||''.''||table_name = '''||p_table_name||''' and column_name = '''||p_field_name||'''';
  --execute v_sql into v_datatype;
  v_where = coalesce(p_where,'1=1');
  v_sql = 'select max('||p_field_name||')::text from '||p_table_name||' where '||v_where;
  perform ${target_schema}.f_write_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'Find max sql is: '||v_sql, 
   p_location    := v_location); --log function call
  execute v_sql into v_res;
  perform ${target_schema}.f_write_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'END find max value from table '||p_table_name||' in field '||p_field_name||', max value is: '||v_res, 
   p_location    := v_location); --log function call
   return v_res;
  exception when others then 
     raise notice 'Function f_get_max_value for table % and field % finished with error: %',p_table_name,p_field_name,sqlerrm;
     PERFORM ${target_schema}.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Function f_get_max_value for table '||p_table_name||' and field '||p_field_name||' finished with error: '||SQLERRM, 
        p_location    := v_location);
       return null::text;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_get_max_value(text, text, text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_get_max_value(text, text, text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_get_max_value(text, text, text) TO "${owner}";
