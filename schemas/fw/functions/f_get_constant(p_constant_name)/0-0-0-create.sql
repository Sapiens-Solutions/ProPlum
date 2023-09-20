CREATE OR REPLACE FUNCTION fw.f_get_constant(p_constant_name text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Get constant value from table load_constants*/
declare
    v_location text := 'fw.f_get_constant';
    v_res text;
    v_sql text;
BEGIN
     v_sql = 'select constant_value from fw.load_constants where constant_name = '''||fw.f_unify_name(p_name := p_constant_name)||'''';
     execute v_sql into v_res;
     return v_res;
 exception when others then 
  raise notice 'ERROR get constant with name %, ERROR: %',p_constant_name,SQLERRM;
  PERFORM fw.f_write_log(
     p_log_type    := 'ERROR', 
     p_log_message := 'Get constant with name '|| p_constant_name||' finished with error, ERROR: '||SQLERRM, 
     p_location    := v_location);
   return null;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_get_constant(text) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_get_constant(text) TO public;
GRANT ALL ON FUNCTION fw.f_get_constant(text) TO "admin";
