CREATE OR REPLACE FUNCTION fw.f_get_partition_interval(p_table_name text, p_partition_value timestamp DEFAULT NULL::timestamp without time zone)
	RETURNS interval
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function search partition for table*/
DECLARE
  v_location            text := 'fw.f_get_partition_interval';
  v_cnt_partitions      int;
  v_table_name          text;
  v_error               text;
  v_partition_delta_sql text;
  v_partition_delta     interval;
  v_partition_name      text;
  v_interval            interval;
  v_where               text;
  v_sql                 text;
BEGIN

  -- Unify parameters
  v_table_name = fw.f_unify_name(p_table_name);
  --Log
  PERFORM fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START Get partition interval for table '||v_table_name, 
     p_location    := v_location);

  -- check table has partitions
  select count(*)
  into v_cnt_partitions
  from pg_partitions p
  where p.schemaname||'.'||p.tablename = lower(v_table_name);
 
  if v_cnt_partitions <= 1 then
    PERFORM fw.f_write_log(
         p_log_type    := 'SERVICE', 
         p_log_message := 'Table is not partitioned '||v_table_name, 
         p_location    := v_location);
    return null;
  end if;
 
     IF p_partition_value is not null THEN
      v_partition_name = 
         fw.f_partition_name_by_value(
            p_table_name      := v_table_name, 
            p_partition_value := p_partition_value);
     END IF;

     v_where = coalesce(' partitiontablename = '''||v_partition_name||'''',' rnk = 1');

      --Get partition parameter
     v_sql = 
     'SELECT partitionrangeend||''::timestamp-''||partitionrangestart||''::timestamp''
          from (
              select p.*, rank() over (order by partitionrank desc) rnk
              from pg_partitions p
              where p.partitionrank is not null
              and   p.schemaname||''.''||p.tablename = lower('''||v_table_name||''')
              ) q
          where '|| v_where;
      raise notice 'v_sql: %',v_sql;
      execute v_sql INTO v_partition_delta_sql;
     
      -- Execute strings to timestamps
      EXECUTE 'SELECT '||v_partition_delta_sql INTO v_partition_delta;

      -- Define partition interval
      IF v_partition_delta between '28 days'::interval and '31 days'::interval THEN
        v_interval := '1 month'::interval;
      ELSIF v_partition_delta < '28 days'::interval THEN
        v_interval := '1 day'::interval;
      ELSIF v_partition_delta > '32 days'::interval THEN
        v_interval := '1 year'::interval;
      ELSIF v_partition_delta < '24 hour'::interval THEN
        v_interval := v_partition_delta::interval; 
      else
        v_interval := null;
        v_error := 'Unable to define partition interval';
        PERFORM fw.f_write_log(
           p_log_type    := 'ERROR', 
           p_log_message := 'Error while getting partition in table '||v_table_name||':'||v_error, 
           p_location    := v_location);
      END IF;

  -- Log Success
  PERFORM fw.f_write_log(
     p_log_type    :='SERVICE', 
     p_log_message := 'END Get partition interval for table '||v_table_name||' ,interval is '||v_interval, 
     p_location    := v_location);
  return v_interval;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_get_partition_interval(text, timestamp) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_get_partition_interval(text, timestamp) TO public;
GRANT ALL ON FUNCTION fw.f_get_partition_interval(text, timestamp) TO "admin";
