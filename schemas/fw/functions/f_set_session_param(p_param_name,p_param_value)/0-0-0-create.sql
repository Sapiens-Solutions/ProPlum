CREATE OR REPLACE FUNCTION fw.f_set_session_param(p_param_name text, p_param_value text)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function set session param*/
DECLARE
  v_location text := 'fw.f_set_session_param';
  v_res text;
  v_param_name text;
BEGIN
 
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START set session param '||p_param_name||' = '||p_param_value, 
     p_location    := v_location); --log function call
  v_param_name   = fw.f_unify_name(p_name := p_param_name);
  perform set_config(v_param_name, p_param_value::text, false);
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'END set session param '||v_param_name||' = '||p_param_value, 
     p_location    := v_location); --log function call
   return true;
  exception when others then 
     raise notice 'Set session param % = % finished with error: %',p_param_name,p_param_value,sqlerrm;
     PERFORM fw.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Set session param '||p_param_name||' = '||p_param_value||' finished with error: '||SQLERRM, 
        p_location    := v_location);
     return false;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_set_session_param(text, text) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_set_session_param(text, text) TO public;
GRANT ALL ON FUNCTION fw.f_set_session_param(text, text) TO "admin";
