CREATE OR REPLACE FUNCTION ${target_schema}.f_update_columns(p_load_id int8, p_table_from_name text, p_column_list _text, p_table_to_name text DEFAULT NULL::text, p_date_field text DEFAULT NULL::text)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2024*/
/*Function updates columns in target table by merging partitions or direct update*/
/* p_column_list - list of columns to be updated
 * p_table_from_name - source object - view, table, table function 
 * p_table_to_name - optional - target table, where columns will be updated
 * p_date_field - optional - delta or business date column in source and target table, for delta load 
 * flag_update - true - direct update of table, false - merge partitions
 */
declare
	v_location text := '${target_schema}.f_update_columns';
	v_table_from_name text;
	v_table_to_name text;
    v_tmp_table_name text;
    v_date_field text;
    v_where text;
	v_start_date timestamp;
	v_end_date timestamp;
    v_extr_to timestamp;
	v_sql text;
	v_cnt bigint := 0;
    v_columns text;
    v_columns_list text;
    flag_update boolean := false;
    v_merge_key _text;
    v_merge_cols text;
    v_merge_cond text;
    v_schema_name text;
    v_upd_cols text;
begin
    -- update columns in partitioned table (update partition)/update columns in ordinary table (update)
	-- Get table load type
    select li.extraction_from, least(li.extraction_to, current_date), coalesce(p_date_field, ob.delta_field, ob.bdate_field), coalesce(p_table_to_name, ob.object_name), ob.where_clause, ob.merge_key
			    from ${target_schema}.objects ob  inner join 
				     ${target_schema}.load_info li 
				  on ob.object_id = li.object_id    
			   where li.load_id = p_load_id
    into v_start_date, v_end_date,v_date_field,v_table_to_name,v_where, v_merge_key;
	v_table_from_name = ${target_schema}.f_unify_name(p_table_from_name);
    if v_merge_key is null then
       PERFORM ${target_schema}.f_write_log(
	       p_log_type    := 'ERROR', 
	       p_log_message := 'Load object with load_id = '||p_load_id||' finished with error. Merge key can''t be null', 
	       p_location    := v_location,
	       p_load_id     := p_load_id);
	    perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
	    return false;
    end if;
    --temp table name
    v_tmp_table_name = right(v_table_to_name,length(v_table_to_name) - position('.' in v_table_to_name)) ||'_temp';
    v_schema_name = ${target_schema}.f_get_table_schema(v_table_to_name);
    v_schema_name = replace(replace(replace(v_schema_name,'src_',''),'stg_',''),'load_','');
    v_schema_name = coalesce(${target_schema}.f_get_constant('c_stg_table_schema'),'stg_')||v_schema_name;-- temp table schema name
	perform ${target_schema}.f_write_log(
		p_log_type    := 'INFO', 
		p_log_message := 'START ' || v_location || ' with load_id = ' || p_load_id || ' from '|| v_start_date || ' to ' || v_end_date||', source - '||v_table_from_name||', target - '||v_table_to_name, 
		p_location    := v_location,
		p_load_id     := p_load_id);
	-- get columns list
   	select string_agg(case when col.column_name = ANY(p_column_list) then 'src.' else 'trg.' end||col.column_name,',' order by ordinal_position) from information_schema.columns col 
      where col.table_schema||'.'||col.table_name = v_table_to_name into v_columns;
    -- get merge condition
	select string_agg('src.'||mk||' = trg.'||mk,' and '),string_agg('src.'||mk,', ') from unnest(v_merge_key) mk into v_merge_cond,v_merge_cols;
    select string_agg('src.'||uc,', ') from unnest(p_column_list) uc into v_upd_cols; 
    --raise notice 'v_columns: %, v_merge_cond: %',v_columns,v_merge_cond;
	v_sql = 
	 ${target_schema}.f_replace_variables(p_load_id,
	 'select '||
	    decode(flag_update,true, v_merge_cols
	    ||coalesce(', src.'||v_date_field,'')||', '
	    ||v_upd_cols,v_columns) ||
	  ' from '||v_table_from_name||' src '||
	    decode(flag_update,true, '', ' join '||v_table_to_name||' trg on '|| v_merge_cond ||
	    ' where'||case when trim(coalesce(v_where,''))='' 
	                   then coalesce(' src.'||v_date_field||' >=  '''||v_start_date||''' and src.'||v_date_field||' < '''||v_end_date||'''',' 1=1')
	                   else ' ('||v_where||')'
	               end));
    raise notice 'sql for temp table: %',v_sql;
    v_cnt = ${target_schema}.f_create_obj_sql(
        p_schema_name := v_schema_name, 
        p_obj_name := v_tmp_table_name, 
        p_sql_text := v_sql, 
        p_grants_template := v_table_to_name, 
        p_mat_flg := true, 
        p_temporary := false, 
        p_distr_cls := array_to_string(v_merge_key,','));
     v_tmp_table_name = v_schema_name||'.'||v_tmp_table_name;
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
			p_log_type    := 'INFO', 
			p_log_message := 'START update columns'|| array_to_string(p_column_list,', ') ||' in ' || v_table_to_name || ' from '||v_tmp_table_name, 
			p_location    := v_location,
			p_load_id     := p_load_id);
	 v_cnt = 0;
     if flag_update then
	      v_sql = 'select '||array_to_string(v_merge_key,', ')||','||array_to_string(p_column_list,', ')||' from '||v_tmp_table_name;
		  perform ${target_schema}.f_write_log(
			p_log_type    := 'INFO', 
			p_log_message := 'Update sql is: '||v_sql, 
			p_location    := v_location,
			p_load_id     := p_load_id); 
          v_cnt = 
		   ${target_schema}.f_update_table_sql(
		     p_table_to_name := v_table_to_name, 
		     p_sql := v_sql, 
		     p_column_list := p_column_list, 
		     p_merge_key := v_merge_key);
		  
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
   		  --gather statistics on updated columns
		  perform ${target_schema}.f_analyze_table(v_table_to_name||'('||array_to_string(p_column_list,',')||')');
		  -- max delta value in load table for load_id
		  v_extr_to = coalesce(${target_schema}.f_get_max_value(
		     p_table_name := v_tmp_table_name, 
		     p_field_name := v_date_field,
		     p_where := v_where),v_end_date);
	 else
		  v_cnt = ${target_schema}.f_load_delta_partitions(
	        p_load_id         := p_load_id, 
	        p_table_from_name := v_tmp_table_name,
	        p_table_to_name   := v_table_to_name,
	        p_merge_partitions:= true,
	        p_where           := '1=1');
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
	   end if;
	   perform ${target_schema}.f_write_log(
		 p_log_type    := 'INFO', 
		 p_log_message := 'END ' || v_location || ' with load_id = ' || p_load_id || ', ' || v_cnt || ' rows updated', 
		 p_location    := v_location,
		 p_load_id     := p_load_id);
	   -- max delta value in load table for load_id
	   v_extr_to = coalesce(${target_schema}.f_get_max_value(
	     p_table_name := v_tmp_table_name,
	     p_field_name := v_date_field)::timestamp,v_end_date);
     perform ${target_schema}.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'row_cnt',
        p_value      := v_cnt::text);
     perform ${target_schema}.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'extraction_to',
        p_value      := v_extr_to::text);
	return true;
	exception when others then
		perform ${target_schema}.f_write_log(
			p_log_type    := 'ERROR', 
			p_log_message := 'Run ' || v_location || ' finished with error: '||SQLERRM, 
			p_location    := v_location,
			p_load_id     := p_load_id);
		return false;
end;
$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_update_columns(int8, text, _text, text, text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_update_columns(int8, text, _text, text, text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_update_columns(int8, text, _text, text, text) TO "${owner}";
