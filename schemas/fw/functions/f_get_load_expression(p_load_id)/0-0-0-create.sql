CREATE OR REPLACE FUNCTION fw.f_get_load_expression(p_load_id int8)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function get load sql for stg table loading*/
DECLARE
  v_location  text := 'fw.f_get_load_expression';
  v_full_table_name text;
  v_tmp_table_name text;
  v_ext_table_name text;
  v_sql       text;
  v_transform jsonb;
BEGIN
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START Get load expression for load_id = '||p_load_id, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call

  v_sql := 'select ob.object_name, ob.transform_mapping
            from fw.load_info li, fw.objects ob where li.object_id = ob.object_id and li.load_id = ' ||
            p_load_id::text;
  execute v_sql into v_full_table_name, v_transform;
  v_full_table_name  = fw.f_unify_name(p_name := v_full_table_name); -- full table name
  v_ext_table_name = fw.f_get_ext_table_name(p_load_id := p_load_id);
  v_tmp_table_name = fw.f_get_delta_table_name(p_load_id := p_load_id);
 
  select 'select '|| string_agg(coalesce(v_transform->>c.column_name,c.column_name)||' '||c.column_name,',' order by c.ordinal_position) ||' from '||v_ext_table_name from information_schema.columns c 
   where c.table_schema||'.'||c.table_name = v_tmp_table_name
   into v_sql;
 
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'END Get load expression for load_id = '||p_load_id||', load sql is: '||coalesce(v_sql,'{empty}'), 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  return v_sql;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_get_load_expression(int8) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_get_load_expression(int8) TO public;
GRANT ALL ON FUNCTION fw.f_get_load_expression(int8) TO "admin";
