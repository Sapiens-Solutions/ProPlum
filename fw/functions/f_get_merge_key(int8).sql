CREATE OR REPLACE FUNCTION ${target_schema}.f_get_merge_key(p_object_id int8)
	RETURNS _text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function get merge_key from objects*/
DECLARE
  v_location text := '${target_schema}.f_get_merge_key';
  v_table_name text;
  v_merge_key text[];
BEGIN
  select merge_key
  into v_merge_key 
  from  ${target_schema}.objects ob
  where object_id = p_object_id
  and merge_key is not null;
  RETURN v_merge_key;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_get_merge_key(int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_get_merge_key(int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_get_merge_key(int8) TO "${owner}";
