CREATE OR REPLACE FUNCTION fw.f_create_date_partitions(p_table_name text, p_partition_value timestamp)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function create new partition for table*/
DECLARE
  v_location            text := 'fw.f_create_date_partitions';
  v_cnt_partitions      int;
  v_table_name          text;
  v_error               text;
  v_partition_end_sql   text;
  v_partition_end       timestamp;
  v_partition_delta_sql text;
  v_partition_delta     interval;
  v_ts_format           text := 'YYYY-MM-DD HH24:MI:SS';
  v_interval            interval;
BEGIN

  -- Unify parameters
  v_table_name = fw.f_unify_name(p_table_name);

  --Log
  PERFORM fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START Creating partitions for table '||v_table_name, 
     p_location    := v_location);
  PERFORM fw.f_write_log('DEBUG', 'v_table_name:{'||v_table_name||'}', v_location);

  IF p_partition_value is null THEN
      v_error := 'Partition value should not be null';
      PERFORM fw.f_write_log(
         p_log_type    := 'ERROR', 
         p_log_message := 'Error while creating partition in table '||p_table_name||':'||v_error, 
         p_location    := v_location);
      RAISE EXCEPTION '% for table % partition %', v_error, v_table_name, v_partition_end_sql;
  END IF;

  -- check table has partitions
  select count(*)
  into v_cnt_partitions
  from pg_partitions p
  where p.schemaname||'.'||p.tablename = lower(v_table_name);
  
  If v_cnt_partitions > 1 THEN
    LOOP
      --Get last partition parameters
      SELECT  partitionrangeend,  partitionrangeend||'::timestamp-'||partitionrangestart||'::timestamp'
      INTO v_partition_end_sql, v_partition_delta_sql
          from (
              select p.*, rank() over (order by partitionrank desc) rnk
              from pg_partitions p
              where p.partitionrank is not null
              and   p.schemaname||'.'||p.tablename = lower(v_table_name)
              ) q
          where rnk = 1;
      
      PERFORM fw.f_write_log(
         p_log_type    := 'DEBUG', 
         p_log_message := 'v_partition_end_sql:{'||v_partition_end_sql||'}', 
         p_location    := v_location);
      PERFORM fw.f_write_log(
         p_log_type    := 'DEBUG', 
         p_log_message := 'v_partition_delta_sql:{'||v_partition_delta_sql||'}', 
         p_location    := v_location);

      -- Execute strings to timestamps
      EXECUTE 'SELECT '||v_partition_end_sql INTO v_partition_end;
      EXECUTE 'SELECT '||v_partition_delta_sql INTO v_partition_delta;

      PERFORM fw.f_write_log(
         p_log_type    := 'DEBUG', 
         p_log_message := 'v_partition_end:{'||v_partition_end||'}', 
         p_location    := v_location);
      PERFORM fw.f_write_log(
         p_log_type    := 'DEBUG', 
         p_log_message := 'v_partition_delta:{'||v_partition_delta||'}', 
         p_location    := v_location);

      -- IF partition exists, THEN exit
      EXIT when v_partition_end > p_partition_value;

      -- Define partition interval
      IF v_partition_delta between '28 days'::interval and '31 days'::interval THEN
        v_interval := '1 month'::interval;
      ELSIF v_partition_delta < '28 days'::interval THEN
        v_interval := '1 day'::interval;
      ELSIF v_partition_delta > '32 days'::interval THEN
        v_interval := '1 year'::interval;
      ELSE
        v_error := 'Unable to define partition interval ';
        PERFORM fw.f_write_log('ERROR', 'Error while creating partition in table '||p_table_name||':'||v_error, v_location);
        RAISE EXCEPTION '% for table % partition %',v_error, v_table_name,v_partition_end_sql;
      END IF;

      PERFORM fw.f_write_log(
         p_log_type    := 'DEBUG', 
         p_log_message := 'v_interval:{'||v_interval||'}', 
         p_location    :=  v_location);
      -- Add partition
      EXECUTE 'ALTER TABLE '||v_table_name||' SPLIT DEFAULT PARTITION START ('||v_partition_end_sql||') END ('''||to_char(v_partition_end+v_interval, v_ts_format)||'''::timestamp)';

      PERFORM fw.f_write_log(
         p_log_type    := 'SERVICE', 
         p_log_message := 'Created partition '||v_partition_end_sql||' for table '||v_table_name, 
         p_location    := v_location);

    END LOOP;
  ELSE
      PERFORM fw.f_write_log(
         p_log_type    := 'SERVICE', 
         p_log_message := 'Table is not partitioned '||v_table_name, 
         p_location    := v_location);
  End if;

  -- Log Success
  PERFORM fw.f_write_log(
     p_log_type    :='SERVICE', 
     p_log_message := 'END Created partitions for table '||v_table_name, 
     p_location    := v_location);

END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_create_date_partitions(text, timestamp) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_create_date_partitions(text, timestamp) TO public;
GRANT ALL ON FUNCTION fw.f_create_date_partitions(text, timestamp) TO "admin";
