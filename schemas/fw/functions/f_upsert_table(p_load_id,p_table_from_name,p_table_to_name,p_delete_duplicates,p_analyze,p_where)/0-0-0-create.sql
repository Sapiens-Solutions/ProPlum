CREATE OR REPLACE FUNCTION fw.f_upsert_table(p_load_id int8, p_table_from_name text, p_table_to_name text, p_delete_duplicates bool DEFAULT false, p_analyze bool DEFAULT true, p_where text DEFAULT NULL::text)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function merges one table to another by delete insert*/
DECLARE
    v_location          text := 'fw.f_upsert_table';
    v_table_from_name   text;
    v_table_to_name     text;
    v_merge_key_arr     text[];
    v_merge_key         text;
    v_delete_query		text;
    v_insert_query		text;
    v_object_id         int8;
    v_delta_fld         text;
    v_cnt               int8;
    v_table_cols        text;
    v_where             text;
begin
	
--Upsert rows from source table (p_table_from_name) into target table (p_table_to_name) using "merge key" from object settings 
--1. Delete all rows from target table which exist in source table
--2. Insert all rows from source table into target table
  --Log
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Start upsert table ' || p_table_to_name ||' from '||p_table_from_name, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  v_table_from_name   = fw.f_unify_name(p_name := p_table_from_name);
  v_table_to_name     = fw.f_unify_name(p_name := p_table_to_name); 
  v_where = coalesce(p_where,'1=1');
  select o.object_id, o.delta_field  from fw.load_info li 
   inner join fw.objects o on li.object_id = o.object_id
  where li.load_id = p_load_id into v_object_id,v_delta_fld;
  v_merge_key_arr     = fw.f_get_merge_key(p_object_id := v_object_id);
  if v_merge_key_arr is null then
    perform fw.f_write_log(
      p_log_type    := 'ERROR', 
      p_log_message := 'ERROR while upsert table ' || p_table_to_name ||' from '||p_table_from_name||', merge key for object is null', 
      p_location    := v_location,
      p_load_id     := p_load_id); --log function call
    raise exception 'Merge key for object % is null',v_object_id;
  end if;
 v_merge_key := array_to_string(v_merge_key_arr,',');
 -- get target table columns
  SELECT string_agg(column_name, ','  ORDER BY ordinal_position) 
    INTO v_table_cols
   FROM information_schema.columns
   WHERE table_schema||'.'||table_name  = v_table_to_name;
  
  --Generate script for Deletion
  select 'DELETE FROM '||v_table_to_name||' USING '||v_table_from_name||' WHERE '||v_where||
          array_to_string(array(select replace(' AND ('||v_table_from_name||'.MERGE_KEY IS NOT DISTINCT FROM '||v_table_to_name||'.MERGE_KEY) ' 
		  , 'MERGE_KEY', unnest(v_merge_key_arr))), ' ') into v_delete_query;
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Upsert table ' || p_table_to_name ||' with delete query: '||v_delete_query, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  EXECUTE v_delete_query;
  GET DIAGNOSTICS v_cnt = ROW_COUNT;
  raise notice 'Table % was updated successfully. % rows deleted.',p_table_to_name,v_cnt;  
   --Generate script for insert (delete duplicates from source by merge key )
  v_insert_query = 'SELECT '||v_table_cols||'
    FROM (
    SELECT *'||
    case when p_delete_duplicates then 
      ', row_number() over (partition by '||v_merge_key||' order by '||coalesce(v_delta_fld,'1')||' desc) as rnk FROM '||v_table_from_name||' where '||v_where||') q WHERE rnk = 1'
    else 
      ' FROM '||v_table_from_name||' where '||v_where||') t'
    end ;
   perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Upsert table ' || p_table_to_name ||' with insert query: '||v_insert_query, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  --Execute insert query
  v_cnt =  fw.f_insert_table_sql(
     p_sql        := v_insert_query,
     p_table_to   := v_table_to_name); --load from stage (delta) table into target 
  if p_analyze is true then
    perform fw.f_analyze_table(p_table_name := v_table_to_name); 
  end if;
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'End upsert table '||p_table_to_name||' from '||p_table_from_name, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  return v_cnt;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_upsert_table(int8, text, text, bool, bool, text) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_upsert_table(int8, text, text, bool, bool, text) TO public;
GRANT ALL ON FUNCTION fw.f_upsert_table(int8, text, text, bool, bool, text) TO "admin";
