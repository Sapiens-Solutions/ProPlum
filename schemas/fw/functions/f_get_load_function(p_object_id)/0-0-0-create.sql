CREATE OR REPLACE FUNCTION fw.f_get_load_function(p_object_id int8)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function get load function for object */
DECLARE
  v_location text := 'fw.f_get_load_function';
  v_function_name text; 

BEGIN
 
perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START get load function for object '||p_object_id, 
     p_location    := v_location); --log function call

select load_function_name from fw.objects ob where ob.object_id = p_object_id
 into v_function_name; -- get load function
   
perform fw.f_write_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'END get load function for object '||p_object_id||', load function is : '||coalesce(v_function_name::text,'{empty}'), 
   p_location    := v_location); --log function call
 return v_function_name;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_get_load_function(int8) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_get_load_function(int8) TO public;
GRANT ALL ON FUNCTION fw.f_get_load_function(int8) TO "admin";
