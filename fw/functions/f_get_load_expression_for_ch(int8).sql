CREATE OR REPLACE FUNCTION ${target_schema}.f_get_load_expression_for_ch(p_load_id int8)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
	/*Function get load sql for stg table loading*/

	/*Changed by Shushkov Stanislav - 21.08.2024 for DEMO Stand*/
DECLARE
  v_location  text := '${target_schema}.f_get_load_expression_for_ch';
  v_full_table_name text;
  v_tmp_table_name text;
  v_ext_table_name text;
  v_sql       text;
  v_transform jsonb;
BEGIN
  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START Get load expression for load_id = '||p_load_id, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call

  v_sql := 'select ob.object_name, ob.transform_mapping
            from ${target_schema}.load_info li, ${target_schema}.objects ob where li.object_id = ob.object_id and li.load_id = ' ||
            p_load_id::text;
  execute v_sql into v_full_table_name, v_transform;
 
  v_full_table_name  = ${target_schema}.f_unify_name(p_name := v_full_table_name); -- full table name
  -- v_ext_table_name = coalesce(${target_schema}.f_get_constant('c_ext_ch_table_prefix'),'ext_ch_')||v_full_table_name||'_'||p_load_id   -- ${target_schema}.f_get_ext_table_name(p_load_id := p_load_id);
  -- v_tmp_table_name = ${target_schema}.f_get_delta_table_name(p_load_id := p_load_id);
 
  select 'select * from '||v_full_table_name --------- changed 
  into v_sql;
  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'END Get load expression for load_id = '||p_load_id||', load sql is: '||coalesce(v_sql,'{empty}'), 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  return v_sql;
END;




$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_get_load_expression_for_ch(int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_get_load_expression_for_ch(int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_get_load_expression_for_ch(int8) TO "${owner}";
