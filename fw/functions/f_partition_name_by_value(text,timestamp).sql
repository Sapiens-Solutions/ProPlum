CREATE OR REPLACE FUNCTION ${target_schema}.f_partition_name_by_value(p_table_name text, p_partition_value timestamp)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function returns partition name of the table by it's value*/
DECLARE
  v_location text := '${target_schema}.f_partition_name_by_value';
  v_table_name text;
  v_partition_name text;
BEGIN
  v_table_name = ${target_schema}.f_unify_name(p_name := p_table_name);
  select max(partname) from ${target_schema}.f_partition_name_list_by_date(v_table_name,p_partition_value,p_partition_value)
  into v_partition_name;
  PERFORM ${target_schema}.f_write_log(
    p_log_type    := 'SERVICE', 
    p_log_message := 'Partition name: '||coalesce(v_partition_name,'{empty}'), 
    p_location    :=  v_location);
  RETURN v_partition_name;

END;
$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_partition_name_by_value(text, timestamp) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_partition_name_by_value(text, timestamp) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_partition_name_by_value(text, timestamp) TO "${owner}";