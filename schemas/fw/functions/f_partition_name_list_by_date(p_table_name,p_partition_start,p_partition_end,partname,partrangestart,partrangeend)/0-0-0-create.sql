CREATE OR REPLACE FUNCTION fw.f_partition_name_list_by_date(p_table_name text, p_partition_start timestamp, p_partition_end timestamp)
	RETURNS TABLE (partname text, partrangestart timestamp, partrangeend timestamp)
	LANGUAGE sql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function returns partition name list of the table by date range*/
  select partitiontablename::text, 
		 split_part(partitionrangestart, '::', 1)::timestamp as partitionrangestart,
		 split_part(partitionrangeend, '::', 1)::timestamp as partitionrangeend
	from pg_partitions
   where lower(schemaname||'.'||tablename) = lower(p_table_name)
	 and partitionisdefault = false
	 and ((split_part(partitionrangestart, '::', 1)::timestamp between to_timestamp(to_char(p_partition_start, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS') and to_timestamp(to_char(p_partition_end, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS')) 
		or ( split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second' between to_timestamp(to_char(p_partition_start, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS') and to_timestamp(to_char(p_partition_end, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS')) 
		or ( to_timestamp(to_char(p_partition_start, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS') between split_part(partitionrangestart, '::', 1)::timestamp and split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second') 
		or ( to_timestamp(to_char(p_partition_end, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS') between split_part(partitionrangestart, '::', 1)::timestamp and split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second'))
   order by partitionposition;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_partition_name_list_by_date(text, timestamp, timestamp) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_partition_name_list_by_date(text, timestamp, timestamp) TO public;
GRANT ALL ON FUNCTION fw.f_partition_name_list_by_date(text, timestamp, timestamp) TO "admin";
