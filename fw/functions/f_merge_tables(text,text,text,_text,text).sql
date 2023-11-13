CREATE OR REPLACE FUNCTION ${target_schema}.f_merge_tables(p_table_from_name text, p_table_to_name text, p_where text, p_merge_key _text, p_trg_table text DEFAULT NULL::text)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function merges one table to another by replacing*/
DECLARE
    v_location          text := '${target_schema}.f_merge_tables';
    v_table_from_name   text;
    v_table_to_name     text;
    v_buffer_table_name text;
    v_buf_prefix        text;
    v_error             text;
    v_part_count        int;
    v_data_exists       int;
    v_table_cols        text;
    v_merge_key_arr     _text;
    v_merge_key         text;
    v_merge_sql         text;
    v_cnt               int8; 
    v_where             text;
BEGIN
    v_table_from_name   = ${target_schema}.f_unify_name(p_name := p_table_from_name);
    v_table_to_name     = ${target_schema}.f_unify_name(p_name := p_table_to_name);
    -- Log
    v_where = coalesce(p_where,'1=1');
    PERFORM ${target_schema}.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'START Merge data from '||v_table_from_name||' to '||v_table_to_name || ' with condition: '||v_where, 
       p_location    := v_location);
      
    -- Create buffer table if necessary
    if p_trg_table is null then 
       v_buf_prefix = ${target_schema}.f_get_constant('c_buf_table_prefix');
       v_buffer_table_name =  ${target_schema}.f_create_tmp_table(p_table_name := v_table_to_name, p_prefix_name := v_buf_prefix);
    --change owner and grant select to buffer
       PERFORM ${target_schema}.f_grant_select(
          p_trg_table_name := v_buffer_table_name, 
          p_src_table      := v_table_to_name);
    else 
       v_buffer_table_name = p_trg_table;
       perform ${target_schema}.f_truncate_table(p_table_name := v_buffer_table_name);
    end if;
   
    -- Check that data exists in source table
    execute '
        select count(*)
        from(
            select *
            from '||v_table_from_name||' where '||v_where||'
            --limit 1
        ) q' into v_data_exists;
    IF v_data_exists = 0 then -- no data found, finish function
        PERFORM ${target_schema}.f_write_log(
           p_log_type    := 'SERVICE', 
           p_log_message := 'No data in source table while merge from '||v_table_from_name||' to '||v_table_to_name,
           p_location    := v_location);
        return 0;
    END IF;
        PERFORM ${target_schema}.f_write_log(
           p_log_type    := 'SERVICE', 
           p_log_message := v_data_exists||' records will be updated in table '||v_table_to_name,
           p_location    := v_location);
    -- Get all columns list
    SELECT string_agg(column_name, ','  ORDER BY ordinal_position) 
     INTO v_table_cols
    FROM information_schema.columns
     WHERE table_schema||'.'||table_name  = v_table_to_name;
    PERFORM ${target_schema}.f_write_log(
       p_log_type    := 'DEBUG', 
       p_log_message := 'v_table_cols:{'||v_table_cols||'}', 
       p_location    := v_location);
    -- Get merge key from table
    v_merge_key_arr := p_merge_key;

    if v_merge_key_arr is null THEN
        PERFORM ${target_schema}.f_write_log(
           p_log_type    := 'ERROR', 
           p_log_message := 'No merge key is defined for table '||v_table_to_name, 
           p_location    := v_location);
        RAISE EXCEPTION 'No merge key is defined for table % ', v_table_to_name;
    end if;
    v_merge_key := array_to_string(v_merge_key_arr,',');
    PERFORM ${target_schema}.f_write_log(
       p_log_type    := 'DEBUG',
       p_log_message := 'v_merge_key:{'||v_merge_key||'}', 
       p_location    := v_location);
    -- Create merge statement
    v_merge_sql :=
    'INSERT INTO '||v_buffer_table_name||'
    SELECT '||v_table_cols||'
    FROM (
    SELECT q.*, rank() over (partition by '||v_merge_key||' order by rnk) as rnk_f
    FROM (
        SELECT '||v_table_cols||', 1 rnk
        FROM '||v_table_from_name||' f where '||v_where||'
        UNION ALL
        SELECT '||v_table_cols||', 2 rnk
        FROM '||v_table_to_name||' t
        ) q
    ) qr
    WHERE rnk_f = 1';
    PERFORM ${target_schema}.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'Begin merge from '||v_table_from_name||' to '||v_table_to_name||'('||v_merge_sql||')', 
       p_location    := v_location);
    execute v_merge_sql;
    GET DIAGNOSTICS v_cnt = ROW_COUNT;
    -- Log
    PERFORM ${target_schema}.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'END Merge data from '||v_table_from_name||' to '||v_table_to_name||', result table: '||v_buffer_table_name, 
       p_location    := v_location);
    return v_data_exists;   
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_merge_tables(text, text, text, _text, text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_merge_tables(text, text, text, _text, text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_merge_tables(text, text, text, _text, text) TO "${owner}";
