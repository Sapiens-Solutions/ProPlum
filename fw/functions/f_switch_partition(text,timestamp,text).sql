CREATE OR REPLACE FUNCTION ${target_schema}.f_switch_partition(p_table_name text, p_partition_value timestamp, p_switch_table_name text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function switch table and partition*/
DECLARE
  v_location text := '${target_schema}.f_switch_partition';
  v_partition_name text;
  v_switch_table_name text;
  v_table_name text;
  v_error text;
BEGIN

  v_switch_table_name = ${target_schema}.f_unify_name(p_name := p_switch_table_name);
  v_table_name = ${target_schema}.f_unify_name(p_name := p_table_name);
  -- find partition
  v_partition_name := ${target_schema}.f_partition_name_by_value(p_table_name, p_partition_value);
  PERFORM ${target_schema}.f_write_log(
    p_log_type    := 'DEBUG', 
    p_log_message := 'v_partition_name:{'||v_partition_name||'}', 
    p_location    := v_location);

  -- We can't switch partition IF we can't find partition
  IF v_partition_name is null THEN
    v_error := 'Unable to switch partition: could not find partition for value '||to_char(p_partition_value, 'YYYY-MM-DD HH24:MI:SS');
    PERFORM  ${target_schema}.f_write_log(
      p_log_type    := 'ERROR', 
      p_log_message := 'Error while switching partition '||v_partition_name||' with table '||v_switch_table_name||':'||v_error, 
      p_location    :=  v_location);
    RAISE EXCEPTION '% for table % partition %',v_error, v_table_name,v_partition_name;
  END IF;
  
  -- switch partition
  PERFORM ${target_schema}.f_switch_partition(
         p_table_name     := v_table_name, 
         p_partition_name := v_partition_name, 
         p_switch_table_name := v_switch_table_name);
  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'End switch partitions for date '|| p_partition_value ||' in table '||v_table_name, 
     p_location    := v_location); --log function call
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_switch_partition(text, timestamp, text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_switch_partition(text, timestamp, text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_switch_partition(text, timestamp, text) TO "${owner}";
