CREATE OR REPLACE FUNCTION fw.f_execute_function(p_function_name text, p_load_id int8 DEFAULT NULL::bigint)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function execute function with params*/
DECLARE
  v_location text := 'fw.f_execute_function';
  v_sql text; 
  v_res text;
  v_function_name text;
  v_function_name_wo text;
  v_start_date timestamp;
  v_end_date timestamp;
  v_extr_from timestamp;
  v_extr_to timestamp;
  v_full_table_name text;
  v_load_type text;
  v_extraction_type text;
  v_object_id int8;
  v_cnt int8;
BEGIN
  --v_function_name = fw.f_unify_name(p_name := p_function_name);
  v_function_name = p_function_name;
  raise notice '1. v_function_name: %',v_function_name;
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START run function '||p_function_name, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  v_function_name_wo = left(v_function_name,position('(' in v_function_name)-1); 
  raise notice '2. v_function_name_wo: %',v_function_name_wo;
  v_sql = 'select count(1) from information_schema.routines where routine_schema ||''.''||routine_name = '''||v_function_name_wo||'''';
  raise notice '3. select count(1) from information_schema.routines v_sql: %',v_sql;
  execute v_sql into v_cnt;
  if coalesce(v_cnt,0) = 0 then 
  perform fw.f_write_log(
     p_log_type    := 'ERROR',
     p_log_message := 'Function '||p_function_name ||'doesnt exist!!!',
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
   return false;
  end if;
  if p_load_id is not null then
    v_sql := 'select ob.object_name, coalesce(li.extraction_type, ob.extraction_type), coalesce(li.load_type, ob.load_type), li.load_from, li.load_to, li.extraction_from, li.extraction_to, ob.object_id 
              from fw.load_info li, fw.objects ob where li.object_id = ob.object_id and li.load_id = ' ||p_load_id::text;
    execute v_sql into v_full_table_name, v_extraction_type, v_load_type, v_start_date, v_end_date, v_extr_from, v_extr_to, v_object_id;
    v_sql = 'select '||replace(replace(replace(replace(replace(replace(replace(replace(v_function_name,
                                                    '$load_from',''''||coalesce(v_start_date,'1000-01-01')::text||''''),
                                                    '$load_to',''''||coalesce(v_end_date,'9999-12-31')::text||''''),
                                                    '$extraction_from',''''||coalesce(v_extr_from,'1000-01-01')::text||''''),
                                                    '$extraction_to',''''||coalesce(v_extr_to,'9999-12-31')::text||''''),
                                                    '$load_id',p_load_id::text),
                                                    '$load_type',''''||v_load_type::text||''''),
                                                    '$extraction_type',''''||v_extraction_type::text||''''),
                                                    '$object_id',v_object_id::text);
  else 
    v_sql = 'select '||v_function_name||'::text';
  end if;
  raise notice '4. select %:  v_sql: %',v_function_name, v_sql;
  perform fw.f_write_log(
     p_log_type    := 'DEBUG',
     p_log_message := 'Run function with sql: '||v_sql,
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  
  execute v_sql into v_res;
  raise notice '5. v_res: %',v_res;
  perform fw.f_write_log(
   p_log_type    := 'SERVICE',
   p_log_message := 'END run function '||p_function_name||' finished with result: '||v_res,
   p_location    := v_location,
   p_load_id     := p_load_id); --log function call
  if v_res = 'true' or v_res = 't' then
    return true;
  else -- function must return bool value
    return false;
  end if;
  exception when others then 
     raise notice 'Function f_execute_function for function % finished with error: %',p_function_name,sqlerrm;
     PERFORM fw.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Function f_execute_function for function '||p_function_name||' finished with error: '||SQLERRM, 
        p_location    := v_location);
     return false;  
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_execute_function(text, int8) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_execute_function(text, int8) TO public;
GRANT ALL ON FUNCTION fw.f_execute_function(text, int8) TO "admin";
