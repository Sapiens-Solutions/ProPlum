CREATE OR REPLACE FUNCTION fw.f_partition_name_by_value(p_table_name text, p_partition_value timestamp)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function returns partition name of the table by it's value*/
DECLARE
  v_location text := 'fw.f_partition_name_by_value';
  v_table_name text;
  v_partition_name text;
  v_part_sql text;
  v_ts_format text := 'YYYYMMDDHH24MISS';
BEGIN

  v_table_name = fw.f_unify_name(p_name := p_table_name);
  
  -- prepare sql to find partition
  select 'select max(q.partname) from ('||
     string_agg('select '''||partitiontablename||''' as partname, to_timestamp('''||to_char(p_partition_value, v_ts_format)||''','''||v_ts_format||''') '||
     case when partitionstartinclusive THEN ' >=' else '>' end ||partitionrangestart||' and to_timestamp('''||to_char(p_partition_value, v_ts_format)||''','''||v_ts_format||''') '|| 
     case when partitionendinclusive THEN ' <=' else '<' end ||partitionrangeend||' as eval', ' union all ') || ' ) q where q.eval = true'
  into v_part_sql
  from pg_partitions
  where lower(schemaname||'.'||tablename) = lower(v_table_name)
  and partitionisdefault=false;
  --PERFORM fw.f_write_log(
  --  p_log_type    := 'DEBUG', 
  --  p_log_message := 'v_part_sql:{'||v_part_sql||'}', 
  --  p_location    :=  v_location);
  
  -- find partition
  EXECUTE v_part_sql INTO v_partition_name;
  PERFORM fw.f_write_log(
    p_log_type    := 'DEBUG', 
    p_log_message := 'v_partition_name:{'||v_partition_name||'}', 
    p_location    :=  v_location);
  RETURN v_partition_name;

END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_partition_name_by_value(text, timestamp) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_partition_name_by_value(text, timestamp) TO public;
GRANT ALL ON FUNCTION fw.f_partition_name_by_value(text, timestamp) TO "admin";
