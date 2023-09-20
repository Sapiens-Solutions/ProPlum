CREATE OR REPLACE FUNCTION fw.f_table_exists(p_table_name text)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function return true if table exists*/
DECLARE
  v_location text := 'fw.f_table_exists';
  v_sql text; 
  v_res bool;
  v_cnt int8;
  v_table_name text;
BEGIN
  v_table_name = fw.f_unify_name(p_name := p_table_name);
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START check if table '||p_table_name||' is exists', 
     p_location    := v_location); --log function call
  v_sql = 'select count(1) from information_schema.tables where table_type = ''BASE TABLE'' and table_schema ||''.''||table_name = '''||v_table_name||'''';
  execute v_sql into v_cnt;
  if coalesce(v_cnt,0) = 0 then 
   v_res = false;
  else 
   v_res = true;
  end if;
  perform fw.f_write_log(
   p_log_type    := 'SERVICE',
   p_log_message := 'END check if table '||p_table_name||' is exists - '||v_res,
   p_location    := v_location); --log function call
return v_res;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_table_exists(text) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_table_exists(text) TO public;
GRANT ALL ON FUNCTION fw.f_table_exists(text) TO "admin";
