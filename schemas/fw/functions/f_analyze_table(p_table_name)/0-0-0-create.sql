CREATE OR REPLACE FUNCTION fw.f_analyze_table(p_table_name text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*collect statistic on table*/
DECLARE
  v_location       text := 'fw.f_analyze_table';
  v_table_name     text;
  v_sql            text;
BEGIN

  -- Get table name
  v_table_name := fw.f_unify_name(p_name := p_table_name);

  perform fw.f_write_log(
     p_log_type := 'SERVICE', 
     p_log_message := 'START analyze table '||v_table_name, 
     p_location    := v_location); --log function call
  v_sql := 'ANALYZE '||v_table_name;
  EXECUTE v_sql;
  perform fw.f_write_log(
     p_log_type := 'SERVICE', 
     p_log_message := 'END analyze table '||v_table_name, 
     p_location    := v_location); --log function call

END

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_analyze_table(text) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_analyze_table(text) TO public;
GRANT ALL ON FUNCTION fw.f_analyze_table(text) TO "admin";
