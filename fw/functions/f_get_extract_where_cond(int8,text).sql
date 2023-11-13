CREATE OR REPLACE FUNCTION ${target_schema}.f_get_extract_where_cond(p_load_id int8, p_table_alias text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function get final where condition for load_id from object settings*/
DECLARE
  v_location text := '${target_schema}.f_get_extract_where_cond';
  v_where text;
  v_where_obj text;
  v_sql   text;
  v_extraction_type text;
  v_delta_fld text;
  v_delta_fld_format text;
  v_delta_fld_type text;
  v_bdate_fld text;
  v_bdate_fld_format text;
  v_bdate_fld_type text;
  v_object_id int8;
  v_full_table_name text;
  v_start_date timestamp;
  v_end_date timestamp;
  v_start_date_c text;
  v_end_date_c text;
  v_alias text;
  v_column_map jsonb;
  v_transform_map jsonb;
  r record;
BEGIN
 perform ${target_schema}.f_write_log(p_log_type    := 'SERVICE', 
                         p_log_message := 'START Get extraction where condition for load_id '||p_load_id, 
                         p_location    := v_location,
                         p_load_id     := p_load_id); --log function call
   v_alias = coalesce(p_table_alias||'.','');
    -- Get table load type
   v_sql := 'select ob.object_name, coalesce(li.extraction_type, ob.extraction_type), li.extraction_from, li.extraction_to,
             ob.delta_field, coalesce(ob.delta_field_format,''YYYY-MM-DD hh:mi:ss''), 
             ob.bdate_field, coalesce(ob.bdate_field_format,''YYYY-MM-DD hh:mi:ss''), ob.object_id,
             ob.column_name_mapping, ob.transform_mapping
             from ${target_schema}.load_info li, ${target_schema}.objects ob where li.object_id = ob.object_id and li.load_id = '||p_load_id::text;
   execute v_sql into v_full_table_name, v_extraction_type, v_start_date, v_end_date, v_delta_fld, v_delta_fld_format, v_bdate_fld, v_bdate_fld_format,v_object_id, v_column_map, v_transform_map;
   v_full_table_name  = ${target_schema}.f_unify_name(p_name := v_full_table_name); -- full table name
   v_where_obj = ${target_schema}.f_get_where_clause(p_object_id := v_object_id);
    --get delta and bdate fields type or transformation
   --select to_char(now(),'YYYY-MM-DD hh:mi:ss');
   select coalesce(data_type,'timestamp') from information_schema.columns c where c.table_schema||'.'||c.table_name = v_full_table_name and c.column_name = v_delta_fld into v_delta_fld_type;
   select coalesce(data_type,'timestamp') from information_schema.columns c where c.table_schema||'.'||c.table_name = v_full_table_name and c.column_name = v_bdate_fld into v_bdate_fld_type;

    if v_where_obj is not null then 
    -- replace common variables in where condition
     v_where_obj = replace(replace(replace(v_where_obj,
            '$extraction_from',''''||v_start_date||''''),
            '$extraction_to',  ''''||v_end_date||''''),
            '$load_id',p_load_id::text);
    end if;
   --raise notice 'v_where_obj is %', v_where_obj;
   IF v_extraction_type = 'FULL' then
      v_where = coalesce(v_where_obj,'1=1');
   ELSIF v_extraction_type = 'DELTA' then
      if v_where_obj is not null then 
         v_where = v_where_obj;
      else 
         v_start_date_c = to_char(v_start_date,v_delta_fld_format);
         v_end_date_c   = to_char(v_end_date,v_delta_fld_format);
         --v_where = coalesce(v_alias||v_delta_fld ||' >= '''|| v_start_date||'''::'||v_delta_fld_type||'
         --            and '||v_alias||v_delta_fld ||' <  '''|| v_end_date  ||'''::'||v_delta_fld_type,'1=1');
         v_where = coalesce(v_alias||v_delta_fld ||' >= '''|| v_start_date_c||''' and '||v_alias||v_delta_fld ||' <  '''|| v_end_date_c  ||'''','1=1');
      end if;
   ELSIF v_extraction_type = 'PARTITION' THEN
      if v_where_obj is not null then 
         v_where = v_where_obj;
      else 
         v_start_date_c = to_char(v_start_date,v_bdate_fld_format);
         v_end_date_c   = to_char(v_end_date,v_bdate_fld_format);
         --v_where = coalesce(v_alias||v_bdate_fld ||' >= '''|| v_start_date||'''::'||v_bdate_fld_type||' 
         --            and '||v_alias||v_bdate_fld ||' <  '''|| v_end_date  ||'''::'||v_bdate_fld_type,'1=1');
         v_where = coalesce(v_alias||v_bdate_fld ||' >= '''|| v_start_date_c||''' and '||v_alias||v_bdate_fld ||' <  '''|| v_end_date_c  ||'''','1=1');
      end if;
   ELSE
      v_where = '1=1';
   END IF;
   -- find fields transformation mapping
  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Start transform mapping for where condition: '||v_where, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  FOR r IN
      SELECT column_name FROM information_schema.columns c WHERE c.table_schema||'.'||c.table_name = v_full_table_name
  loop
      if (strpos(v_where,r.column_name) > 0) and ((v_column_map->>r.column_name)::text is not null) then --search for transform mapping in where condition
       v_where = replace(v_where,r.column_name,v_column_map->>r.column_name);
      end if;
  END LOOP;
  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'END Get extraction where condition for load_id = '||p_load_id||', where condition is: '||coalesce(v_where,'empty'), 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  return v_where;
END;



$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_get_extract_where_cond(int8, text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_get_extract_where_cond(int8, text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_get_extract_where_cond(int8, text) TO "${owner}";
