CREATE OR REPLACE FUNCTION ${target_schema}.f_get_pxf_partition(p_load_id int8)
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*get pxf partition string*/
DECLARE
  v_location     text := '${target_schema}.f_get_pxf_partition';
  v_extraction_type  text;
  v_bdate_field text;
  v_delta_field text;
  v_bdate_field_type text;
  v_delta_field_type text;
  v_bdate_field_format text;
  v_delta_field_format text;
  v_extraction_from date;
  v_extraction_to date;
  v_part_string text;
  v_range int4;
  v_node_cnt int4 := 16;
  v_conn_string text;
  v_column_map jsonb;
BEGIN

  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Start get pxf partition for load_id = '||p_load_id, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call

     select li.extraction_type, coalesce(column_name_mapping->>ob.bdate_field,ob.bdate_field),coalesce(column_name_mapping->>ob.delta_field,ob.delta_field), coalesce(li.extraction_from,'1980-01-01'::date), coalesce(li.extraction_to,'2100-01-01'::date),
            ob.connect_string, ob.column_name_mapping, ob.delta_field_type, ob.bdate_field_type, ob.delta_field_format, ob.bdate_field_format 
     from ${target_schema}.load_info li 
      join ${target_schema}.objects ob on ob.object_id = li.object_id
     where li.load_id = p_load_id
    into v_extraction_type, v_bdate_field, v_delta_field,v_extraction_from,v_extraction_to, v_conn_string, v_column_map, v_delta_field_type, v_bdate_field_type, v_delta_field_format, v_bdate_field_format;
   v_range = (v_extraction_to - v_extraction_from)/v_node_cnt;
   v_delta_field = replace(v_delta_field,'"','');
   v_bdate_field = replace(v_bdate_field,'"','');
   PERFORM ${target_schema}.f_write_log(
     p_log_type    := 'DEBUG', 
     p_log_message := 'Variables: v_bdate_field: '||coalesce(v_bdate_field,'{empty}')::text||', v_delta_field: '||coalesce(v_delta_field,'{empty}')::text, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
   if v_range <= 0 then 
    v_range = 1;
   end if;
   case 
	   when v_extraction_type in ('FULL') then
	    v_part_string = '';
	   when v_extraction_type in ('DELTA')
	    and v_delta_field is not null 
	    and v_extraction_from is not null 
	    and v_extraction_to is not null 
	    and position('&PARTITION_BY' in v_conn_string) = 0 
	   then
         if v_range <= 0 then 
           v_part_string = '';
         else
           if v_delta_field_type is null or v_delta_field_type like 'date%' or v_delta_field_type like 'timestamp%' then
            v_part_string = '&PARTITION_BY='||v_delta_field||':date&RANGE='||v_extraction_from||':'||v_extraction_to||'&INTERVAL='||v_range||':day';
           else 
            v_part_string = ${target_schema}.f_get_enum_partition(p_field_name := v_delta_field, p_format:= v_delta_field_format, p_start_date := v_extraction_from, p_end_date := v_extraction_to);
           end if;
         end if;
       when v_extraction_type in ('PARTITION') 
        and v_bdate_field is not null 
        and v_extraction_from is not null 
        and v_extraction_to is not null 
        and position('&PARTITION_BY' in v_conn_string) = 0 
       then 
         if v_range <= 0 then 
           v_part_string = '';
         else
           if v_bdate_field_type is null or v_bdate_field_type like 'date%' or v_bdate_field_type like 'timestamp%' then
            v_part_string = '&PARTITION_BY='||v_bdate_field||':date&RANGE='||v_extraction_from||':'||v_extraction_to||'&INTERVAL='||v_range||':day';
           else 
            v_part_string = ${target_schema}.f_get_enum_partition(p_field_name := v_bdate_field, p_format:= v_bdate_field_format, p_start_date := v_extraction_from, p_end_date := v_extraction_to);
           end if;
         end if;
       else 
         v_part_string = '';
    end case;
    -- Log 
  PERFORM ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'END get pxf partition for load_id = '||p_load_id||' , partition string: '||v_part_string , 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  return v_part_string;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_get_pxf_partition(int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_get_pxf_partition(int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_get_pxf_partition(int8) TO "${owner}";
