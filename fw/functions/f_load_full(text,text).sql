CREATE OR REPLACE FUNCTION ${target_schema}.f_load_full(p_trg_table text, p_src_table text)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function switch tables and create backup table*/
DECLARE
  v_location text := '${target_schema}.f_load_full';
  v_trg_table text;
  v_src_table text;
  v_bkp_table_name text;
  v_bkp_prefix_name text;
  v_schema_name text;
  v_cnt int8;
BEGIN
  -- Unify names
  v_trg_table := ${target_schema}.f_unify_name(p_name := p_trg_table);
  v_src_table := ${target_schema}.f_unify_name(p_name := p_src_table);
  -- Define buffer table name
  v_schema_name = left(v_trg_table,position('.' in v_trg_table)-1); -- target table schema name
  v_schema_name = replace(replace(v_schema_name,'src_',''),'stg_','');-- bkp table schema name
  v_bkp_prefix_name = coalesce(${target_schema}.f_get_constant('c_bkp_table_prefix'),'bkp_');
  perform ${target_schema}.f_write_log(p_log_type    := 'SERVICE', 
                          p_log_message := 'START Switch tables '||v_trg_table||' with table '||v_src_table, 
                          p_location    := v_location); --log function call
  execute 'select count(1) from 
            (select * from '||v_src_table||' limit 1) cnt' into v_cnt;
  if v_cnt = 0 then --source table is empty, stop processing
    PERFORM ${target_schema}.f_write_log(
       p_log_type    := 'SERVICE',  
       p_log_message := 'END Switch table '||v_trg_table||' with table '||v_src_table||', source table is empty',
       p_location    := v_location);
    return 0;
  end if;
  
  -- Lock table
  EXECUTE 'LOCK TABLE '||v_trg_table||' in ACCESS EXCLUSIVE MODE';
  --create backup of target table
  v_bkp_table_name = ${target_schema}.f_create_tmp_table
          (
           p_table_name  := v_trg_table,
           p_schema_name := 'stg_'||v_schema_name,
           p_prefix_name := v_bkp_prefix_name,
           p_suffix_name := null::text);
   perform ${target_schema}.f_insert_table(
             p_table_from := v_trg_table,
             p_table_to   := v_bkp_table_name);
  -- truncate target table
   perform ${target_schema}.f_truncate_table(v_trg_table);
  -- Insert data
   v_cnt =  ${target_schema}.f_insert_table(
             p_table_from := v_src_table,
             p_table_to   := v_trg_table);
   PERFORM ${target_schema}.f_analyze_table(p_table_name := v_trg_table);--Analyze table
   PERFORM ${target_schema}.f_write_log(
      p_log_type    := 'SERVICE',  
      p_log_message := 'END Switch table '||v_trg_table||' with table '||v_src_table,
      p_location    := v_location);
   return v_cnt;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_load_full(text, text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_load_full(text, text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_load_full(text, text) TO "${owner}";
