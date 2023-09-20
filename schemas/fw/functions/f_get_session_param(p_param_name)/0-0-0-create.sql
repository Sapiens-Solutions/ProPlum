CREATE OR REPLACE FUNCTION fw.f_get_session_param(p_param_name text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function returns session parameter*/
DECLARE
  v_location text := 'fw.f_get_session_param';
  v_res text;
  v_param_name text;
BEGIN
   v_param_name = fw.f_unify_name(p_name := p_param_name);
   v_res = current_setting(v_param_name);
   return v_res;
   exception when others then 
    return null::text;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_get_session_param(text) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_get_session_param(text) TO public;
GRANT ALL ON FUNCTION fw.f_get_session_param(text) TO "admin";
