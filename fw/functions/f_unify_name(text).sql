CREATE OR REPLACE FUNCTION ${target_schema}.f_unify_name(p_name text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function unifies table name, column name and other names*/
DECLARE
BEGIN
  RETURN lower(trim(translate(p_name, ';/''','')));
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_unify_name(text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_unify_name(text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_unify_name(text) TO "${owner}";
