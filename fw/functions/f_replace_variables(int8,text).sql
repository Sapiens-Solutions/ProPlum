CREATE OR REPLACE FUNCTION ${target_schema}.f_replace_variables(p_load_id int8, p_string text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
/*Ismailov Dmitry
* Sapiens Solutions 
* 2024*/
/*Function replace common variables in input string*/
DECLARE
  v_location    text := '${target_schema}.f_replace_variables';
  v_table_name  text; 
  v_sql         text;
  v_extraction_type text;
  v_load_type   text;
  v_extr_from   timestamp;
  v_extr_to     timestamp;
  v_load_from   timestamp;
  v_load_to     timestamp;
  v_object_id   int8;
  v_res         bool;
BEGIN
  v_sql := 'select ob.object_name, 
                   coalesce(li.extraction_type, ob.extraction_type), 
                   coalesce(li.load_type, ob.load_type), 
                   li.load_from, 
                   li.load_to, 
                   li.extraction_from, 
                   li.extraction_to, 
                   ob.object_id 
            from ${target_schema}.load_info li, ${target_schema}.objects ob where li.object_id = ob.object_id and li.load_id = ' ||p_load_id;
  execute v_sql into v_table_name, v_extraction_type, v_load_type, v_load_from, v_load_to, v_extr_from, v_extr_to, v_object_id;
  v_sql = replace(replace(replace(replace(replace(replace(replace(replace(p_string,
                                                    '$load_from',''''||coalesce(v_load_from,'1900-01-01')::text||''''),
                                                    '$load_to',''''||coalesce(v_load_to,'2999-12-31')::text||''''),
                                                    '$extraction_from',''''||coalesce(v_extr_from,'1900-01-01')::text||''''),
                                                    '$extraction_to',''''||coalesce(v_extr_to,'2999-12-31')::text||''''),
                                                    '$load_id',p_load_id::text),
                                                    '$load_type',''''||v_load_type::text||''''),
                                                    '$extraction_type',''''||v_extraction_type::text||''''),
                                                    '$object_id',v_object_id::text);
  return v_sql;
END;
$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_replace_variables(int8,text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_replace_variables(int8,text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_replace_variables(int8,text) TO "${owner}";