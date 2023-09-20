CREATE OR REPLACE FUNCTION fw.f_update_table_sql(p_table_to_name text, p_sql text, p_column_list _text, p_merge_key _text)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function merges sql to another table by update*/
DECLARE
    v_location      text := 'fw.f_update_table_sql';
    v_sql           text;
    v_table_to_name text;
    v_cnt           int8;
    v_merge_key     text;
begin
	
--Update rows from source sql (p_sql) into target table (p_table_to_name) using "merge key" from object settings 

  --Log
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Start update table ' || p_table_to_name ||' from sql '||p_sql,
     p_location    := v_location); --log function call
     
  v_table_to_name   = fw.f_unify_name(p_name := p_table_to_name);
  if p_column_list is null then 
   perform fw.f_write_log(
      p_log_type    := 'ERROR',
      p_log_message := 'ERROR while update table ' || v_table_to_name ||', column list is null', 
      p_location    := v_location); --log function call
   return null;
  end if;
 
  if p_merge_key is null then
    perform fw.f_write_log(
      p_log_type    := 'ERROR', 
      p_log_message := 'ERROR while update table ' ||v_table_to_name||', merge key for object is null', 
      p_location    := v_location); --log function call
    raise notice 'Merge key is null';
    return null;
  end if;

  --Generate script for update
  v_sql = 'with update_sql as ('||p_sql||') update '||v_table_to_name|| ' as trg'||
          ' set '||array_to_string(array(select replace('UPD_COLUMN = update_sql.UPD_COLUMN','UPD_COLUMN', unnest(p_column_list))), ',')||
          ' from update_sql '||
          ' where 1=1 '||array_to_string(array(select replace('AND (trg.MERGE_KEY = update_sql.MERGE_KEY) ','MERGE_KEY', unnest(p_merge_key))), ' ');

  perform fw.f_write_log(
     p_log_type    := 'DEBUG', 
     p_log_message := 'Update sql v_sql = '||v_sql,
     p_location    := v_location); --log function call
  --execute update query
  execute v_sql;
 
  GET DIAGNOSTICS v_cnt = ROW_COUNT;
  raise notice '% rows updated from sql: % into %',v_cnt, v_sql,v_table_to_name;   

  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'End update table '||v_table_to_name||' from sql: '||p_sql,
     p_location    := v_location); --log function call
  return v_cnt;
 exception when others then 
  raise notice 'ERROR update table % from sql: %, ERROR: %',v_table_to_name,p_sql,SQLERRM;
  PERFORM fw.f_write_log(
     p_log_type    := 'ERROR', 
     p_log_message := 'update table '|| v_table_to_name||' from sql: '||p_sql||' finished with error'', ERROR: '||SQLERRM, 
     p_location    := v_location);
   return false;
 
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_update_table_sql(text, text, _text, _text) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_update_table_sql(text, text, _text, _text) TO public;
GRANT ALL ON FUNCTION fw.f_update_table_sql(text, text, _text, _text) TO "admin";
