CREATE OR REPLACE FUNCTION ${target_schema}.f_load_simple(p_load_id int8, p_src_table text, p_trg_table text DEFAULT NULL::text, p_delete_duplicates bool DEFAULT false)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function starts simple load function */
DECLARE
  v_location  text := '${target_schema}.f_load_simple';
  v_object_id int8;
  v_cnt       int8;
  v_src_table text;
  v_trg_table text;
  v_tmp_table text;
  v_buf_table text;
  v_schema    text;
  v_extraction_type text;
  v_load_type text;
  v_res       bool;
  v_end_date  timestamp;
  v_delta_fld text;
  v_bdate_fld text;
  v_load_from timestamp;
  v_load_to   timestamp;
  v_where     text;
  v_extr_sql  text;
  v_merge_key _text;
BEGIN
 -- function load upsert data from source table into target
 perform ${target_schema}.f_set_session_param(
    p_param_name := '${target_schema}.load_id', 
    p_param_value := p_load_id::text);
 select ob.object_id, ob.object_name, li.extraction_type, li.load_type,li.load_to,ob.delta_field,ob.bdate_field, li.load_from, li.load_to
   from ${target_schema}.objects ob  inner join 
	    ${target_schema}.load_info li 
	 on ob.object_id = li.object_id    
   where li.load_id  = p_load_id
   into v_object_id, v_trg_table, v_extraction_type, v_load_type, v_end_date,v_delta_fld,v_bdate_fld,v_load_from,v_load_to; -- get object_id, target table, load_type
  v_src_table  = ${target_schema}.f_unify_name(p_name := p_src_table);
  v_trg_table  = coalesce(${target_schema}.f_unify_name(p_name := p_trg_table),v_trg_table);
  v_schema     = ${target_schema}.f_get_table_schema(p_table := v_trg_table);
  v_tmp_table  = ${target_schema}.f_create_tmp_table(
    p_table_name  := v_trg_table, 
    p_schema_name := 'stg_'||v_schema,
    p_prefix_name := 'tmp_', 
    p_drop_table  := true,
    p_is_temporary := false);
 perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START simple load from table '||v_src_table||' into table '||v_trg_table||' with load_id = '||p_load_id ||', extraction_type = '||coalesce(v_extraction_type,'{empty}')||', load_type = '||coalesce(v_load_type,'{empty}'), 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  v_extr_sql = ${target_schema}.f_get_extr_expression(
    p_load_id      := p_load_id, 
    p_source_table := v_src_table,
    p_trg_table    := v_tmp_table);
  --extract data from source into stage
   --v_where := ${target_schema}.f_get_extract_where_cond(p_load_id := p_load_id);
   --v_extr_sql = v_extr_sql||' where '||v_where;

  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Extraction from  '||v_src_table||' into table '||v_tmp_table||' with sql: '||v_extr_sql, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  v_cnt = ${target_schema}.f_insert_table_sql(
     p_table_to := v_tmp_table, 
     p_sql      := v_extr_sql, 
     p_truncate_tgt := true);
  if v_cnt is null then 
    raise notice 'ERROR Load object with load_id = %',p_load_id;
    PERFORM ${target_schema}.f_write_log(
       p_log_type    := 'ERROR', 
       p_log_message := 'Load object with load_id = '||p_load_id||' finished with error', 
       p_location    := v_location,
       p_load_id     := p_load_id);
    perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
    return false;
  end if;
  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Extraction from  '||v_src_table||' into table '||v_tmp_table||', '|| v_cnt||' rows extracted', 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  if v_cnt is not null then
     v_res = true;
     perform ${target_schema}.f_update_load_info(
        p_load_id    := p_load_id, 
        p_field_name := 'extraction_to', 
        p_value      := coalesce(${target_schema}.f_get_max_value(v_tmp_table,v_delta_fld)));
  else 
     v_res = false;
  end if;
  --load data from stage into target
  --v_where = coalesce(${target_schema}.f_get_where_clause(p_object_id := v_object_id),'1=1');
  v_where = '1=1'; -- where condition applied to extraction, load all data from stage
  case 
   when v_load_type = 'FULL' then 
     v_res = ${target_schema}.f_load_simple_full(
        p_load_id   := p_load_id, 
        p_src_table := v_tmp_table,
        p_trg_table := v_trg_table);
   when v_load_type = 'DELTA_UPSERT' then 
     v_res = ${target_schema}.f_load_simple_upsert(
        p_load_id   := p_load_id, 
        p_src_table := v_tmp_table,
        p_trg_table := v_trg_table,
        p_delete_duplicates := p_delete_duplicates,
        p_where     := v_where);
     v_end_date = least(coalesce(${target_schema}.f_get_max_value(v_tmp_table,v_delta_fld)::timestamp,v_end_date),v_end_date);
     perform ${target_schema}.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'load_to',
        p_value      := v_end_date::text);
   when v_load_type = 'DELTA_UPDATE' then 
     v_res = ${target_schema}.f_load_simple_update(
        p_load_id   := p_load_id, 
        p_src_table := v_tmp_table,
        p_trg_table := v_trg_table,
        p_delete_duplicates := p_delete_duplicates,
        p_where     := v_where);
     v_end_date = least(coalesce(${target_schema}.f_get_max_value(v_tmp_table,v_delta_fld)::timestamp,v_end_date),v_end_date);
     perform ${target_schema}.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'load_to',
        p_value      := v_end_date::text);
   when v_load_type = 'PARTITION' then 
     v_cnt = ${target_schema}.f_load_delta_partitions(
        p_load_id         := p_load_id, 
        p_table_from_name := v_tmp_table,
        p_table_to_name   := v_trg_table,
        p_merge_partitions:= false,
        p_where           := v_where,
        p_delete_duplicates := p_delete_duplicates);
     if v_cnt is null then
       v_res = false;
       perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
       return false;
     end if;
     v_end_date = least(coalesce(${target_schema}.f_get_max_value(v_tmp_table,v_bdate_fld)::timestamp,v_end_date),v_end_date);
     perform ${target_schema}.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'load_to',
        p_value      := v_end_date::text);
     perform ${target_schema}.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'row_cnt',
        p_value      := v_cnt::text);
     v_res = true;
   when v_load_type = 'DELTA_MERGE' then 
     v_merge_key = ${target_schema}.f_get_merge_key(p_object_id := v_object_id);
     v_buf_table = ${target_schema}.f_create_tmp_table(
       p_table_name  := v_trg_table, 
       p_schema_name := ${target_schema}.f_get_constant('c_stg_table_schema')||${target_schema}.f_get_table_schema(v_trg_table), 
       p_prefix_name := 'buf_',
       p_drop_table := true);
     v_cnt = ${target_schema}.f_merge_tables(
        p_table_from_name := v_tmp_table,
        p_table_to_name   := v_trg_table, 
        p_where     := v_where, 
        p_merge_key := v_merge_key, 
        p_trg_table := v_buf_table);
     perform ${target_schema}.f_switch_def_partition(
        p_table_from_name := v_buf_table,
        --p_table_to_name := v_tmp_table);
        p_table_to_name := v_trg_table);       
     v_end_date = least(coalesce(${target_schema}.f_get_max_value(v_tmp_table,v_delta_fld)::timestamp,v_end_date),v_end_date);
     perform ${target_schema}.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'load_to',
        p_value      := v_end_date::text);
     if v_cnt is null then
       v_res = false;
       perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
       return false;
     end if;
     perform ${target_schema}.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'row_cnt',
        p_value      := v_cnt::text);
     PERFORM ${target_schema}.f_analyze_table(p_table_name := v_trg_table);
     v_res = true;
   when v_load_type = 'DELTA' then 
     v_cnt = ${target_schema}.f_insert_table(
        p_table_from := v_tmp_table,
        p_table_to   := v_trg_table,
        p_where      := v_where); --Insert data from stg to target table
     if v_cnt is null then
       v_res = false;
       perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
       return false;
     end if;
     --Analyze table
     PERFORM ${target_schema}.f_analyze_table(p_table_name := v_trg_table);
     v_end_date = least(coalesce(${target_schema}.f_get_max_value(v_tmp_table,v_delta_fld)::timestamp,v_end_date),v_end_date);
     perform ${target_schema}.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'load_to',
        p_value      := v_end_date::text);
     perform ${target_schema}.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'row_cnt',
        p_value      := v_cnt::text);
     v_res = true;
   when v_load_type = 'UPDATE_PARTITION' then 
     v_cnt = ${target_schema}.f_load_delta_partitions(
        p_load_id         := p_load_id, 
        p_table_from_name := v_tmp_table,
        p_table_to_name   := v_trg_table,
        p_merge_partitions:= true,
        p_where           := v_where,
        p_delete_duplicates := p_delete_duplicates);
     if v_cnt is null then
       v_res = false;
       perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
       return false;
     end if;
     v_end_date = least(coalesce(${target_schema}.f_get_max_value(v_tmp_table,v_delta_fld)::timestamp,v_end_date),v_end_date);
     perform ${target_schema}.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'load_to',
        p_value      := v_end_date::text);
     perform ${target_schema}.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'row_cnt',
        p_value      := v_cnt::text);
     v_res = true;
   else
     perform ${target_schema}.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'ERROR no such load_type '||v_load_type, 
        p_location    := v_location,
        p_load_id     := p_load_id); --log function call
     v_res = false;
     perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
   end case;
 perform ${target_schema}.f_write_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'END simple load from table '||v_src_table||' into table '||v_trg_table||' with load_id = '||p_load_id, 
   p_location    := v_location,
   p_load_id     := p_load_id); --log function call
   if v_res is true then 
    perform ${target_schema}.f_set_load_id_success(p_load_id := p_load_id);  
   else
    perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id); 
   end if;
 return v_res;
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

ALTER FUNCTION ${target_schema}.f_load_simple(int8, text, text, bool) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_load_simple(int8, text, text, bool) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_load_simple(int8, text, text, bool) TO "${owner}";