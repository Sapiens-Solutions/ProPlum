CREATE OR REPLACE FUNCTION ${target_schema}.f_get_partition_key(p_table_name text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function return partition field name*/
DECLARE
    v_location text := '${target_schema}.f_get_partition_key';
    v_table_name text;
    v_part_attr_name text;
BEGIN
    
    v_table_name = ${target_schema}.f_unify_name(p_table_name);
    -- Get partition key column
    select columnname
    into v_part_attr_name
    from pg_catalog.pg_partition_columns
    where lower(schemaname||'.'||tablename) = lower(v_table_name);
    PERFORM ${target_schema}.f_write_log('DEBUG', 'v_part_attr_name:{'||coalesce(v_part_attr_name,'empty')||'}', v_location);
    RETURN v_part_attr_name;

END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_get_partition_key(text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_get_partition_key(text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_get_partition_key(text) TO "${owner}";
