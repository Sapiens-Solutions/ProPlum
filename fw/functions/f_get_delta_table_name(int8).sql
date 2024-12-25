CREATE OR REPLACE FUNCTION ${target_schema}.f_get_delta_table_name(p_load_id int8)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function get delta table name*/
DECLARE
  v_location  text := '${target_schema}.f_get_delta_table_name';
  v_full_table_name text;
  v_object_id int8;
  v_tmp_table_name text;
  v_table_name text;
  v_schema_name text;
  v_sql       text;
BEGIN
  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START Get delta table name for load_id = '||p_load_id, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call

  v_sql := 'select ob.object_name, ob.object_id
            from ${target_schema}.load_info li, ${target_schema}.objects ob where li.object_id = ob.object_id and li.load_id = ' ||
            p_load_id::text;
  execute v_sql into v_full_table_name, v_object_id; 
  v_full_table_name  = ${target_schema}.f_unify_name(p_name := v_full_table_name); -- full table name
  v_schema_name = left(v_full_table_name,position('.' in v_full_table_name)-1); -- target table schema name
  v_schema_name = replace(replace(replace(v_schema_name,'src_',''),'stg_',''),'load_','');
  v_schema_name = coalesce(${target_schema}.f_get_constant('c_stg_table_schema'),'stg_')||v_schema_name;-- delta table schema name
  v_table_name =  right(v_full_table_name,length(v_full_table_name) - POSITION('.' in v_full_table_name));-- table name wo schema
  v_tmp_table_name = v_schema_name||'.'||coalesce(${target_schema}.f_get_constant('c_delta_table_prefix'),'delta_')||v_table_name||'_'||v_object_id;--||'_'||p_load_id ;
   perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'END Get delta table name for load_id = '||p_load_id||', table name: '||coalesce(v_tmp_table_name,'{empty}'), 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  return v_tmp_table_name;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_get_delta_table_name(int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_get_delta_table_name(int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_get_delta_table_name(int8) TO "${owner}";
