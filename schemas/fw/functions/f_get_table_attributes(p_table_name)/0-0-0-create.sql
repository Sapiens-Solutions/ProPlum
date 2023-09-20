CREATE OR REPLACE FUNCTION fw.f_get_table_attributes(p_table_name text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
		
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function returns table storage params*/
DECLARE
  v_location text := 'fw.f_get_table_attributes';
  v_params text;
BEGIN
  
	select coalesce('with (' || array_to_string(reloptions, ', ') || ')','')
	from pg_class  
	into v_params
	where oid = fw.f_unify_name(p_name := p_table_name)::regclass;
	return v_params;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_get_table_attributes(text) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_get_table_attributes(text) TO public;
GRANT ALL ON FUNCTION fw.f_get_table_attributes(text) TO "admin";
