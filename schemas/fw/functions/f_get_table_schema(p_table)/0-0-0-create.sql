CREATE OR REPLACE FUNCTION fw.f_get_table_schema(p_table text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*function return input table schema */
DECLARE
  v_location text := 'fw.f_get_table_schema';
  v_table    text;
  v_schema   text;
BEGIN
   v_table = fw.f_unify_name(p_name := p_table);
   v_schema = case 
	when position('.' in v_table) = 0
     then ''
    else
     left(v_table,position('.' in v_table)-1) -- table schema name
   end;
   return v_schema;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_get_table_schema(text) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_get_table_schema(text) TO public;
GRANT ALL ON FUNCTION fw.f_get_table_schema(text) TO "admin";
