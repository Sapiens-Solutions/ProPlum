CREATE OR REPLACE FUNCTION ${target_schema}.f_load_data(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function loads data into target table*/
DECLARE
    v_location            text := '${target_schema}.f_load_data';
    v_end_date            timestamp;
    v_start_date          timestamp;
    v_full_table_name     text;
    v_load_type           text;
    v_partition_key       text;
    v_tmp_table_name      text;
    v_error               text;
    v_sql                 text;
    v_delta_fld           text;
    v_bdate_fld           text;
    v_res                 bool;
    v_cnt                 int8;
    v_load_function       text;
    v_repeat_interval     int4 := 60;
    v_repeat_count        int4 := 20;
BEGIN
	perform ${target_schema}.f_write_log(p_log_type := 'SERVICE', 
       p_log_message := 'START load data into target table for load_id = ' || p_load_id, 
       p_location    := v_location,
       p_load_id     := p_load_id); --log function call
   	--set load_id for session
    perform ${target_schema}.f_set_session_param(
       p_param_name  := '${target_schema}.load_id', 
       p_param_value := p_load_id::text);
      
    -- Get table load type
    v_sql := 'select ob.object_name, coalesce(li.load_type, ob.load_type), li.extraction_from, li.extraction_to, ob.delta_field, ob.bdate_field, ob.load_function_name
              from ${target_schema}.load_info li, ${target_schema}.objects ob where li.object_id = ob.object_id and li.load_id = ' ||
             p_load_id::text;
    execute v_sql into v_full_table_name, v_load_type, v_start_date, v_end_date, v_delta_fld, v_bdate_fld, v_load_function;
    v_full_table_name  = ${target_schema}.f_unify_name(p_name := v_full_table_name); -- full table name
    v_tmp_table_name   = ${target_schema}.f_get_delta_table_name(p_load_id := p_load_id);
    v_partition_key    = ${target_schema}.f_get_partition_key(p_table_name := v_full_table_name);
    --set lock for load process
     while not
      ${target_schema}.f_set_load_lock(
        p_load_id := p_load_id, 
        p_lock_type := 'EXCLUSIVE', 
        p_object_name := v_full_table_name)
     loop
       perform pg_sleep(v_repeat_interval);--interval in seconds
       v_cnt = v_cnt + 1;
       perform ${target_schema}.f_write_log(
          p_log_type    := 'SERVICE',
          p_log_message := 'CONTINUE checking load locks on the table ' || v_full_table_name || '. Step number: ' ||v_cnt::text, 
          p_location    := v_location);
       if v_cnt = v_repeat_count then
         PERFORM ${target_schema}.f_write_log(
            p_log_type    := 'ERROR', 
            p_log_message := 'Number of steps reached the limit: ' || v_cnt::text, 
            p_location    := v_location);
         RAISE exception 'Number of steps reached its limit, terminate load';
       end if;
     end loop;
    v_cnt = 0;
    PERFORM ${target_schema}.f_wait_locks(
       p_table_name      := v_full_table_name, 
       p_repeat_interval := 60,
       p_repeat_count    := 60,
       p_terminate_lock  := true); --wait for no locks on main table every 1 minute 60 times
    IF 
      v_load_type = 'FULL' then
        v_cnt = ${target_schema}.f_load_full(
           p_trg_table := v_full_table_name,
           p_src_table := v_tmp_table_name); --switch tmp and main table
        perform ${target_schema}.f_truncate_table(p_table_name := v_tmp_table_name);
        perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'row_cnt',
           p_value      := v_cnt::text);
        v_res = true;
    ELSIF 
      v_load_type = 'DELTA_MERGE' then
        v_cnt = ${target_schema}.f_load_delta_merge(p_load_id := p_load_id);
        -- set row_cnt in ${target_schema}.load_info
        perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'row_cnt',
           p_value      := v_cnt::text);
        v_start_date = least(coalesce(${target_schema}.f_get_min_value(v_tmp_table_name,v_delta_fld)::timestamp,v_start_date),v_start_date);
        v_end_date   = least(coalesce(${target_schema}.f_get_max_value(v_tmp_table_name,v_delta_fld)::timestamp,v_end_date),v_end_date);
        perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'load_from',
           p_value      := v_start_date::text);
        perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'load_to',
           p_value      := v_end_date::text);
        v_res = true;
    ELSIF 
        v_load_type = 'DELTA' THEN
        IF v_partition_key IS not NULL then
         --Create partitions if needed
         PERFORM ${target_schema}.f_create_date_partitions(
            p_table_name      := v_full_table_name, 
            p_partition_value := v_end_date);
        END IF;
            -- Insert data to target table
         v_cnt = ${target_schema}.f_insert_table(
            p_table_from := v_tmp_table_name,
            p_table_to   := v_full_table_name); --Insert data from stg to target table
            --Analyze table
         PERFORM ${target_schema}.f_analyze_table(p_table_name := v_full_table_name);
         perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'row_cnt',
           p_value      := v_cnt::text);
         v_start_date = least(coalesce(${target_schema}.f_get_min_value(v_tmp_table_name,v_delta_fld)::timestamp,v_start_date),v_start_date);
         v_end_date   = least(coalesce(${target_schema}.f_get_max_value(v_tmp_table_name,v_delta_fld)::timestamp,v_end_date),v_end_date);
         perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'load_from',
           p_value      := v_start_date::text);
         perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'load_to',
           p_value      := v_end_date::text);
         v_res = true;
    ELSIF v_load_type = 'PARTITION' THEN
      IF  v_partition_key IS not NULL then
        -- Load data to target table by exchanging partitions
        v_cnt = ${target_schema}.f_load_delta_partitions(
           p_load_id         := p_load_id,
           p_table_from_name := v_tmp_table_name,
           p_table_to_name   := v_full_table_name,
           p_merge_partitions := false
           );
        perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'row_cnt',
           p_value      := v_cnt::text);
        v_start_date = least(coalesce(${target_schema}.f_get_min_value(v_tmp_table_name,v_bdate_fld)::timestamp,v_start_date),v_start_date);
        v_end_date   = least(coalesce(${target_schema}.f_get_max_value(v_tmp_table_name,v_bdate_fld)::timestamp,v_end_date),v_end_date);
        perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'load_from',
           p_value      := v_start_date::text);
        perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'load_to',
           p_value      := v_end_date::text);
        v_res = true;
      ELSE
        v_error := 'No date partitions in table '||v_full_table_name;
        perform ${target_schema}.f_write_log(
           p_log_type    := 'ERROR', 
           p_log_message := 'Error while processing load data tasks: ' || v_error, 
           p_location    := v_location,
           p_load_id     := p_load_id); --log function call
        RAISE NOTICE '% for load type %',v_error,v_load_type;
        perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);
        return false;
      END IF;
    -- load by iterable update partitions 
    ELSIF v_load_type = 'UPDATE_PARTITION' THEN
      IF  v_partition_key IS not NULL then
        -- Load data to target table by exchanging partitions
        v_cnt = ${target_schema}.f_load_delta_partitions(
           p_load_id         := p_load_id,
           p_table_from_name := v_tmp_table_name,
           p_table_to_name   := v_full_table_name,
           p_merge_partitions := true
           );
        perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'row_cnt',
           p_value      := v_cnt::text);
        v_start_date = least(coalesce(${target_schema}.f_get_min_value(v_tmp_table_name,v_delta_fld)::timestamp,v_start_date),v_start_date);
        v_end_date   = least(coalesce(${target_schema}.f_get_max_value(v_tmp_table_name,v_delta_fld)::timestamp,v_end_date),v_end_date);
        perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'load_from',
           p_value      := v_start_date::text);
        perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'load_to',
           p_value      := v_end_date::text);
        v_res = true;
      ELSE
        v_error := 'No date partitions in table '||v_full_table_name;
        perform ${target_schema}.f_write_log(
           p_log_type    := 'ERROR', 
           p_log_message := 'Error while processing load data tasks: ' || v_error, 
           p_location    := v_location,
           p_load_id     := p_load_id); --log function call
        RAISE NOTICE '% for load type %',v_error,v_load_type;
        perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);
        return false;
      END IF;
--delete insert load type
    ELSIF 
        v_load_type = 'DELTA_UPSERT' then
        v_cnt = ${target_schema}.f_upsert_table(
           p_load_id         := p_load_id,
           p_table_from_name := v_tmp_table_name,
           p_table_to_name   := v_full_table_name);
        v_start_date = least(coalesce(${target_schema}.f_get_min_value(v_tmp_table_name,v_delta_fld)::timestamp,v_start_date),v_start_date);
        v_end_date = least(coalesce(${target_schema}.f_get_max_value(v_tmp_table_name,v_delta_fld)::timestamp,v_end_date),v_end_date);
        perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'load_from',
           p_value      := v_start_date::text);
        perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'load_to',
           p_value      := v_end_date::text);  
        perform ${target_schema}.f_update_load_info(
           p_load_id    := p_load_id,
           p_field_name := 'row_cnt',
           p_value      := v_cnt::text);  
        v_res = true;
    ELSE
        v_error := 'Unable to process load type';
        perform ${target_schema}.f_write_log(
           p_log_type := 'ERROR', 
           p_log_message := 'Error while loading: ' || v_error, 
           p_location    := v_location,
           p_load_id     := p_load_id); --log function call
        RAISE NOTICE '% %',v_error,v_load_type;
        perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);
        perform ${target_schema}.f_delete_load_lock(p_load_id :=  p_load_id);
        return false;
    END IF;  

    if v_res is false then
      PERFORM ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);
      perform ${target_schema}.f_delete_load_lock(p_load_id :=  p_load_id);
      -- Log errors
      perform ${target_schema}.f_write_log(
         p_log_type := 'SERVICE', 
         p_log_message := 'END load data into table ' || v_full_table_name||' finished with error', 
         p_location    := v_location,
         p_load_id     := p_load_id); --log function call
      return false;
     end if;
--if load_function_name is not empty (end routine)
     if v_load_function is not null and replace(v_load_function,' ','') <> '' then
      perform ${target_schema}.f_write_log(
         p_log_type := 'SERVICE', 
         p_log_message := 'START Trying to run function '|| v_load_function, 
         p_location    := v_location,
         p_load_id     := p_load_id); --log function call
      v_res = ${target_schema}.f_load_object(p_load_id);
     end if;
    
    if v_res is true then
      PERFORM ${target_schema}.f_set_load_id_success(p_load_id := p_load_id);
      perform ${target_schema}.f_delete_load_lock(p_load_id :=  p_load_id);
      -- Log Success
      perform ${target_schema}.f_write_log(
         p_log_type := 'SERVICE', 
         p_log_message := 'END load data into table '||v_full_table_name||' for load_id = '|| p_load_id||', '|| v_cnt|| ' rows loaded',
         p_location    := v_location,
         p_load_id     := p_load_id); --log function call
    else 
      PERFORM ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);
      perform ${target_schema}.f_delete_load_lock(p_load_id :=  p_load_id);
      -- Log errors
      perform ${target_schema}.f_write_log(
         p_log_type := 'SERVICE', 
         p_log_message := 'END load data into table ' || v_full_table_name||' finished with error', 
         p_location    := v_location,
         p_load_id     := p_load_id); --log function call
      return false;
    end if;
    return v_res;
    exception when others then 
     raise notice 'ERROR loading table %: %',v_full_table_name,SQLERRM;
     perform ${target_schema}.f_delete_load_lock(p_load_id :=  p_load_id);
     PERFORM ${target_schema}.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Load data into table '||v_full_table_name||' finished with error: '||SQLERRM, 
        p_location    := v_location,
        p_load_id     := p_load_id);
     perform ${target_schema}.f_set_load_id_error(p_load_id := p_load_id);  
     return false;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_load_data(int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_load_data(int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_load_data(int8) TO "${owner}";
