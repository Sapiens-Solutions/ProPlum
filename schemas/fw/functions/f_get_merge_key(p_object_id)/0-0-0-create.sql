CREATE OR REPLACE FUNCTION fw.f_get_merge_key(p_object_id int8)
	RETURNS _text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function get merge_key from objects*/
DECLARE
  v_location text := 'fw.f_get_merge_key';
  v_table_name text;
  v_merge_key text[];
BEGIN
  select merge_key
  into v_merge_key 
  from  fw.objects ob
  where object_id = p_object_id
  and merge_key is not null;
  RETURN v_merge_key;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_get_merge_key(int8) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_get_merge_key(int8) TO public;
GRANT ALL ON FUNCTION fw.f_get_merge_key(int8) TO "admin";
