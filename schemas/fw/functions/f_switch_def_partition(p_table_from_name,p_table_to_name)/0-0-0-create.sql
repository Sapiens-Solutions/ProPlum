CREATE OR REPLACE FUNCTION fw.f_switch_def_partition(p_table_from_name text, p_table_to_name text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function switches default partitions with _buffer table
Support only tables with only one default partition*/
DECLARE
    v_location text := 'fw.f_switch_def_partition';
    v_table_from_name text;
    v_table_to_name text;
    v_part_attr_name text;
    v_part_attr_value timestamp;
    v_data_exists int;
    v_error    text;
	v_owner    text;
BEGIN
    -- Unify parameters
    v_table_from_name = fw.f_unify_name(p_table_from_name);
    v_table_to_name = fw.f_unify_name(p_table_to_name);
    -- Log
    PERFORM fw.f_write_log(
      p_log_type    := 'SERVICE', 
      p_log_message := 'START Switch partition from '||v_table_from_name||' to '||v_table_to_name , 
      p_location    := v_location);
     
	-- Set target table's owner to _tmp table
    perform fw.f_grant_select(
       p_trg_table_name := v_table_from_name, 
       p_src_table := v_table_to_name);
      
    -- Check that data exists in source table
    EXECUTE '
    select count(1)
    from(
        select *
        from '||v_table_from_name||'
        limit 1
    ) q' into v_data_exists;

    IF v_data_exists = 0 THEN
        PERFORM fw.f_write_log(
          p_log_type    := 'SERVICE', 
          p_log_message := 'END No data in source table, exchange stops from '||v_table_from_name||' to '||v_table_to_name, 
          p_location    := v_location);
        RETURN;
    END IF;
	set gp_enable_exchange_default_partition to on; 
        -- Exchange partitions
    EXECUTE 'ALTER TABLE '||v_table_to_name||' EXCHANGE default PARTITION WITH TABLE '||v_table_from_name||' ;';
    --Log
    PERFORM fw.f_write_log(
      p_log_type    := 'SERVICE', 
      p_log_message := 'END Switch partition from '||v_table_from_name||' to '||v_table_to_name , 
      p_location    := v_location);
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_switch_def_partition(text, text) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_switch_def_partition(text, text) TO public;
GRANT ALL ON FUNCTION fw.f_switch_def_partition(text, text) TO "admin";
