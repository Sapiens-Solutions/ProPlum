CREATE OR REPLACE FUNCTION ${target_schema}.f_switch_partition(p_table_name text, p_partition_name text, p_switch_table_name text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function switch table*/
DECLARE
  v_location text := '${target_schema}.f_switch_partition';
  v_partition_name text;
  v_switch_table_name text;
  v_table_name text;
  v_rank int;
  v_error text;
  v_part_name text;
  v_sql text;
  v_owner text;
  v_schema text;
BEGIN

  -- unify parameters
  v_partition_name = ${target_schema}.f_unify_name(p_partition_name);
  v_switch_table_name = ${target_schema}.f_unify_name(p_switch_table_name);
  v_table_name = ${target_schema}.f_unify_name(p_table_name);
  v_schema = substring(v_table_name, 0, position('.' in v_table_name));
    
  -- Log
  PERFORM ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START Switch partition '||v_partition_name||' with table '||v_switch_table_name , 
     p_location    := v_location);

  -- Find rank by partition name
  select partitionrank into v_rank
  from pg_partitions 
  where schemaname||'.'||tablename = lower(v_table_name)
  and   partitiontablename = lower(v_partition_name);
  
  -- rank can't be null
  IF v_rank is null THEN
    v_error := 'Unable to switch partition: could not find rank';
    PERFORM ${target_schema}.f_write_log(
       p_log_type    := 'ERROR', 
       p_log_message := 'Error while switching partition '||v_partition_name||' with table '||v_switch_table_name||':'||v_error,
       p_location    := v_location);
    RAISE EXCEPTION '% for table % partition %',v_error, v_table_name,v_partition_name;
  END IF;

  perform ${target_schema}.f_grant_select(
     p_trg_table_name := v_switch_table_name, 
     p_src_table := v_table_name);
  
  --Switch partition
  EXECUTE 'ALTER TABLE '||v_table_name||' EXCHANGE PARTITION FOR (RANK('||to_char(v_rank,'999999999')||')) WITH TABLE '||v_switch_table_name||' WITH VALIDATION;';

  --	Prepare ANALYZE statement
 	select schemaname||'.'||partitiontablename from pg_partitions 
	into v_part_name
	where partitiontablename = lower(v_partition_name)
      and schemaname         = lower(v_schema);

  perform ${target_schema}.f_analyze_table(v_part_name);

  -- Log 
  PERFORM ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'END Switch partition '||v_partition_name||', rank '||v_rank||' with table '||v_switch_table_name , 
     p_location    := v_location);
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_switch_partition(text, text, text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_switch_partition(text, text, text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_switch_partition(text, text, text) TO "${owner}";
