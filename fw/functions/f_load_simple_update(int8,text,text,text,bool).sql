CREATE OR REPLACE FUNCTION ${target_schema}.f_load_simple_update(p_load_id int8, p_src_table text, p_trg_table text DEFAULT NULL::text, p_where text DEFAULT NULL::text, p_delete_duplicates bool DEFAULT false)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$	
/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2024*/
/*Function load data from stage into target by update
 * merge_key is needed
 * all columns except merge_key, bdate_field and delta field will be updated*/
DECLARE
  v_location      text := '${target_schema}.f_load_simple_update';
  v_object_id     text;
  v_cnt           int8;
  v_src_table     text;
  v_trg_table     text;
  v_where         text;
  v_sql           text;
  v_sql_col       text;
  v_delta_fld     text;
  v_bdate_fld     text;
  v_merge_key     _text;
  v_columns       _text;
  v_extr_type     text;
  v_end_date      timestamp;
BEGIN
 -- function update data from source table into target
 perform ${target_schema}.f_set_session_param(
    p_param_name := '${target_schema}.load_id', 
    p_param_value := p_load_id::text);
 select ob.object_id, ob.object_name, ob.merge_key, ob.delta_field, ob.bdate_field, li.extraction_type,li.extraction_to 
   from ${target_schema}.objects ob  inner join 
	    ${target_schema}.load_info li 
	 on ob.object_id = li.object_id    
   where li.load_id  = p_load_id
   into v_object_id, v_trg_table,v_merge_key,v_delta_fld, v_bdate_fld, v_extr_type,v_end_date; -- get load params
  v_src_table  = ${target_schema}.f_unify_name(p_name := p_src_table);
  v_trg_table  = coalesce(${target_schema}.f_unify_name(p_name := p_trg_table),v_trg_table);
  v_where = coalesce(p_where, '1=1');
 perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START simple update load from table '||v_src_table||' into table '||v_trg_table||' with load_id = '||p_load_id|| ' and condition: '||v_where, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call

 -- get sql expression from source table and columns to update from source table except merge key     
 select  'select '||string_agg(column_name::text,',')||' from '||
    case when p_delete_duplicates 
    then  
    '(select *, row_number() over (partition by '||array_to_string(v_merge_key,',')||' order by '||coalesce(v_delta_fld,'1')||' desc) as rnk 
       FROM '||v_src_table||' where '||v_where||') q WHERE rnk = 1 '
    else 
      v_src_table||' where '||v_where
    end, 
    array_agg(column_name::text) filter(where column_name not in (array_to_string(v_merge_key,',')) and column_name not in (v_bdate_fld,v_delta_fld))
   from (select col.column_name::text from information_schema.columns col where col.table_schema||'.'||col.table_name = v_src_table order by ordinal_position) t
 into v_sql, v_columns;
 
 raise notice 'v_columns = {%}', v_columns;

 if v_sql is null then
    PERFORM ${target_schema}.f_write_log(
     p_log_type    := 'ERROR', 
     p_log_message := 'Load object with load_id = '||p_load_id||' finished with error: cannot find columns for source table '||v_src_table, 
     p_location    := v_location,
     p_load_id     := p_load_id);
    perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
    return false;
 end if;

v_cnt = ${target_schema}.f_update_table_sql(
    p_table_to_name := v_trg_table,
    p_sql           := v_sql, 
    p_column_list   := v_columns, 
    p_merge_key     := v_merge_key);
 if v_cnt is null then
 	return false;
 end if;

 perform ${target_schema}.f_analyze_table(v_trg_table||'('||array_to_string(v_columns,',')||')');
 if  
     v_extr_type = 'DELTA' and v_delta_fld is not null then 
     v_end_date = least(${target_schema}.f_get_max_value(p_table_name:= v_src_table, p_field_name := v_delta_fld, p_where := v_where)::timestamp,v_end_date::timestamp);
 elseif   	
     v_extr_type = 'PARTITION' and v_bdate_fld is not null then 
     v_end_date = least(${target_schema}.f_get_max_value(p_table_name:= v_src_table, p_field_name := v_bdate_fld, p_where := v_where)::timestamp,v_end_date::timestamp);
 end if;
 perform ${target_schema}.f_update_load_info(
   p_load_id    := p_load_id,
   p_field_name := 'extraction_to',
   p_value      := v_end_date::text);
 perform ${target_schema}.f_update_load_info(
    p_load_id    := p_load_id,
    p_field_name := 'row_cnt',
    p_value      := v_cnt::text);
 perform ${target_schema}.f_write_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'END simple update load from table '||v_src_table||' into table '||v_trg_table||' with load_id = '||p_load_id||', '||v_cnt||' rows updated', 
   p_location    := v_location,
   p_load_id     := p_load_id); --log function call
 return true;
 exception when others then 
  raise notice 'ERROR Load object with load_id = %: %',p_load_id,SQLERRM;
  PERFORM ${target_schema}.f_write_log(
     p_log_type    := 'ERROR', 
     p_log_message := 'Load object with load_id = '||p_load_id||' finished with error: '||SQLERRM, 
     p_location    := v_location,
     p_load_id     := p_load_id);
   perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
   return false;
END;
$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_load_simple_update(int8, text, text, text, bool) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_load_simple_update(int8, text, text, text, bool) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_load_simple_update(int8, text, text, text, bool) TO "${owner}";
