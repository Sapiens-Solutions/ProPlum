CREATE OR REPLACE FUNCTION ${target_schema}.f_drop_partition_range(p_table_name text, p_partition_start timestamp, p_partition_end timestamp, p_flag_drop_trunc bool DEFAULT false)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
    rec record;
    v_location text := '${target_schema}.f_drop_partition_range';
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2024*/
/*Function drop partition of the table by date range*/
BEGIN
       perform ${target_schema}.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'Start drop partitions in table ' || p_table_name ||' range from '||p_partition_start||' to '||p_partition_end, 
        p_location    := v_location); --log function call
        
  for rec in
    select partname, partrangestart, partrangeend 
      from ${target_schema}.f_partition_name_list_by_date(p_table_name, p_partition_start, p_partition_end)
  -- Drop partition cycle
   loop
	 if p_flag_drop_trunc is true
	  then
        execute 'alter table '||p_table_name||' drop partition for ('''||rec.partrangestart||'''::date)';
        perform ${target_schema}.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'Drop partitions ' || rec.partname ||' in table ' || p_table_name ||' range from '||rec.partrangestart||' to '||rec.partrangeend, 
        p_location    := v_location); --log function call
       else 
        execute 'truncate table '||rec.partname;
        perform ${target_schema}.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'Truncate partitions ' || rec.partname ||' in table ' || p_table_name ||' range from '||rec.partrangestart||' to '||rec.partrangeend, 
        p_location    := v_location); --log function call
       end if;
   end loop;  
     perform ${target_schema}.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'End drop partitions in table ' || p_table_name ||' range from '||p_partition_start||' to '||p_partition_end, 
       p_location    := v_location); --log function call       
  return true;
 
 exception when others then 
     raise notice 'ERROR drop partition %: %',p_table_name,SQLERRM;
     return false;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_drop_partition_range(text, timestamp, timestamp, bool) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_drop_partition_range(text, timestamp, timestamp, bool) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_drop_partition_range(text, timestamp, timestamp, bool) TO "${owner}";
