CREATE OR REPLACE FUNCTION ${target_schema}.f_get_enum_partition(p_field_name text, p_format text, p_start_date date, p_end_date date)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
	/*get pxf partition in enum format*/
declare
    v_location text := '${target_schema}.f_get_enum_partition';
    v_format text;
    v_field_name text;
    v_enum_part text;
    v_iter_date date;
    v_part_string text;
    v_start_date date;
    v_end_date date;
begin
  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Start get enum partition', 
     p_location    := v_location); --log function call
   v_format = coalesce(p_format,null);
   v_start_date = coalesce(p_start_date,null);
   v_end_date = coalesce(p_end_date,null);
   v_field_name = p_field_name;
   if v_format is null or v_start_date is null or v_end_date is null then 
    v_part_string = '';
    return v_part_string;
   end if;
   v_iter_date = v_start_date;
   v_enum_part = '';
   raise notice 'INFO: Collect partition string: v_iter_date - %, v_load_to - %',v_iter_date, v_end_date;
   loop
     EXIT WHEN v_iter_date > v_end_date;
     v_enum_part = v_enum_part||case when v_iter_date = v_start_date then '' else ':' end||to_char(v_iter_date,v_format);
     --raise notice 'INFO: v_enum_part: <%>',v_enum_part;
     v_iter_date = v_iter_date + interval'1 day';
   end loop;
   v_part_string = '&PARTITION_BY='||v_field_name||':enum&RANGE='||v_enum_part;
   PERFORM ${target_schema}.f_write_log(
      p_log_type    := 'SERVICE', 
      p_log_message := 'END get enum partition: '||v_part_string,
      p_location    := v_location); --log function call
   return v_part_string;
   exception when others then 
    PERFORM ${target_schema}.f_write_log(
       p_log_type    := 'ERROR', 
       p_log_message := 'ERROR while geting enum partition: '||SQLERRM, 
       p_location    := v_location);
    return '';
 END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_get_enum_partition(text, text, date, date) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_get_enum_partition(text, text, date, date) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_get_enum_partition(text, text, date, date) TO "${owner}";
