DROP FUNCTION if exists ${target_schema}.f_create_date_partitions(text, timestamp);

CREATE OR REPLACE FUNCTION ${target_schema}.f_create_date_partitions(p_table_name text, p_partition_value timestamp, p_limit_value timestamp default null::timestamp)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2025*/
/*Function create new partition for table*/

DECLARE
  v_location            text := '${target_schema}.f_create_date_partitions';
  v_cnt_partitions      int;
  v_table_name          text;
  v_error               text;
  v_partition_name      text;
  v_partition_end_sql   text;
  v_partition_end       timestamp;
  v_partition_delta_sql text;
  v_partition_delta     interval;
  v_ts_format           text := 'YYYY-MM-DD HH24:MI:SS';
  v_interval            interval;
  v_min_value           timestamp;
  v_max_value           timestamp;
  v_sql                 text;
BEGIN

  -- Unify parameters
  v_table_name = ${target_schema}.f_unify_name(p_table_name);
  v_max_value = coalesce(p_limit_value,'9999-12-31'::timestamp);
  v_min_value = '1000-01-01'::timestamp;
  
  --Log
  PERFORM ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START Creating partitions for table '||v_table_name||' , partition value: '||p_partition_value||' , limit value: '||v_max_value, 
     p_location    := v_location);
  PERFORM ${target_schema}.f_write_log('DEBUG', 'v_table_name:{'||v_table_name||'}', v_location);

  IF p_partition_value is null THEN
      v_error := 'Partition value should not be null';
      PERFORM ${target_schema}.f_write_log(
         p_log_type    := 'ERROR', 
         p_log_message := 'Error while creating partition in table '||p_table_name||':'||v_error, 
         p_location    := v_location);
      RAISE EXCEPTION '% for table %', v_error, v_table_name;
  END IF;

  -- check table has partitions
  select count(*)
  into v_cnt_partitions
  from pg_partitions p
  where p.schemaname||'.'||p.tablename = lower(v_table_name);
  
  If v_cnt_partitions > 1 THEN
    LOOP
      --Get last partition parameters
	    select partrangeend, (partrangeend-partrangestart)::interval 
	     from ${target_schema}.f_partition_name_list_by_date(v_table_name,v_min_value,v_max_value) 
	    order by partrangeend desc 
	    limit 1
	   into v_partition_end,v_partition_delta;

      PERFORM ${target_schema}.f_write_log(
         p_log_type    := 'DEBUG', 
         p_log_message := 'v_partition_end:{'||v_partition_end||'}', 
         p_location    := v_location);
      PERFORM ${target_schema}.f_write_log(
         p_log_type    := 'DEBUG', 
         p_log_message := 'v_partition_delta:{'||v_partition_delta||'}', 
         p_location    := v_location);

      -- IF partition exists, THEN exit
      EXIT when v_partition_end > p_partition_value;

      -- Define partition interval
      IF v_partition_delta between '28 days'::interval and '31 days'::interval THEN
        v_interval := '1 month'::interval;
        v_partition_name = 'm_'||to_char(v_partition_end,'mm_yyyy');
      ELSIF v_partition_delta < '28 days'::interval THEN
        v_interval := '1 day'::interval;
        v_partition_name = 'd_'||to_char(v_partition_end,'dd_mm_yyyy');
      ELSIF v_partition_delta between '32 days'::interval and '92 days'::interval THEN
        v_interval := '3 month'::interval;
        v_partition_name = 'q_'||to_char(v_partition_end,'Q_yyyy');
      ELSIF v_partition_delta > '92 days'::interval THEN
        v_interval := '1 year'::interval;
        v_partition_name = 'y_'||to_char(v_partition_end,'yyyy');
      ELSE
        v_error := 'Unable to define partition interval ';
        PERFORM ${target_schema}.f_write_log('ERROR', 'Error while creating partition in table '||p_table_name||':'||v_error, v_location);
        RAISE EXCEPTION '% for table % partition %',v_error, v_table_name, v_partition_end;
      END IF;

      PERFORM ${target_schema}.f_write_log(
         p_log_type    := 'DEBUG', 
         p_log_message := 'v_interval:{'||v_interval||'}', 
         p_location    :=  v_location);
      -- Add partition
      --EXECUTE 'ALTER TABLE '||v_table_name||' SPLIT DEFAULT PARTITION START ('||v_partition_end_sql||') END ('''||to_char(v_partition_end+v_interval, v_ts_format)||'''::timestamp)';
      v_sql = 
      'ALTER TABLE '||v_table_name||' SPLIT DEFAULT PARTITION START ('''||v_partition_end||'''::timestamp) END ('''||to_char(v_partition_end+v_interval, v_ts_format)||'''::timestamp)
      INTO (PARTITION '||v_partition_name||', default partition)';
      PERFORM ${target_schema}.f_write_log(
         p_log_type    := 'DEBUG', 
         p_log_message := 'Split partition sql v_sql:{'||v_sql||'}', 
         p_location    :=  v_location);
      
      execute v_sql;

      PERFORM ${target_schema}.f_write_log(
         p_log_type    := 'SERVICE', 
         p_log_message := 'Created partition '||v_partition_name||' for table '||v_table_name, 
         p_location    := v_location);
    END LOOP;
  ELSE
      PERFORM ${target_schema}.f_write_log(
         p_log_type    := 'SERVICE', 
         p_log_message := 'Table is not partitioned '||v_table_name, 
         p_location    := v_location);
  End if;

  -- Log Success
  PERFORM ${target_schema}.f_write_log(
     p_log_type    :='SERVICE', 
     p_log_message := 'END Created partitions for table '||v_table_name, 
     p_location    := v_location);

END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_create_date_partitions(text, timestamp, timestamp) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_create_date_partitions(text, timestamp, timestamp) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_create_date_partitions(text, timestamp, timestamp) TO "${owner}";
