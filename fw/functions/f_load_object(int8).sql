CREATE OR REPLACE FUNCTION ${target_schema}.f_load_object(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function starts load function */
DECLARE
  v_location      text := '${target_schema}.f_load_object';
  v_function_name text; 
  v_object_id     int8;
  v_res           bool;
  v_res_func      text;

BEGIN
 -- function get load function from ${target_schema}.objects and execute it
	
 perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START load object with load_id = '||p_load_id, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
 perform ${target_schema}.f_set_session_param(
    p_param_name := '${target_schema}.load_id', 
    p_param_value := p_load_id::text);
 select ob.object_id 
   from ${target_schema}.objects ob  inner join 
	    ${target_schema}.load_info li 
	 on ob.object_id = li.object_id    
   where li.load_id  = p_load_id
   into v_object_id; -- get object_id function
   
 v_function_name = ${target_schema}.f_get_load_function(p_object_id := v_object_id);
 if v_function_name is not null then 
    v_res_func = ${target_schema}.f_execute_function(
       p_function_name := v_function_name,
       p_load_id       := p_load_id);
      if v_res_func = 'true' or v_res_func = 't' then 
       v_res = true;
       perform ${target_schema}.f_set_load_id_success(p_load_id := p_load_id);  
      elsif v_res_func  = 'false' or v_res_func = 'f' then v_res = false;
      else 
        raise notice 'Function % end with result: %',v_function_name,v_res;
        v_res = false;
        perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
       end if;
  else 
   v_res = false; --no function found
   perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
  end if;

 perform ${target_schema}.f_write_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'END load object with load_id = '||p_load_id||', result is : '||coalesce(v_res_func,'empty'), 
   p_location    := v_location,
   p_load_id     := p_load_id); --log function call
 return v_res;
 exception when others then 
  raise notice 'ERROR Load object with load_id % finished with error: %',p_load_id,SQLERRM;
  PERFORM ${target_schema}.f_write_log(
     p_log_type    := 'ERROR', 
     p_log_message := 'Load object with load_id = '||p_load_id||' finished with error: '||SQLERRM, 
     p_location    := v_location,
     p_load_id     := p_load_id);
   perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
   return false;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_load_object(int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_load_object(int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_load_object(int8) TO "${owner}";
