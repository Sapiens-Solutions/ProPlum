CREATE OR REPLACE FUNCTION fw.f_get_load_id(p_object_id int8, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function returns load_id for dates in parameters
 * if dates empty - return load_id with max date*/
DECLARE
  v_location text := 'fw.f_get_load_id';
  v_table_name text;
  v_error text;
  v_sql text;
  v_load_id_new_status   int  := 1;
  v_load_id_work_status  int  := 2;
  v_load_id              int8;
  v_extr_start           timestamp;
  v_extr_end             timestamp;
  c_minDate              date := to_date('19000101', 'YYYYMMDD');
  c_maxDate              date := to_date('99991231', 'YYYYMMDD');
  v_object_id            int8;
  v_act_time             bool;
begin
	perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'Start get load_id for object ' || p_object_id, 
       p_location    := v_location); 
   /*select activitystart is null or
       activityend is null or
       case activitystart > activityend
        when true
          then current_time > activitystart or current_time < activityend
          else current_time > activitystart and current_time < activityend end,
        object_id
    into v_act_time, v_object_id
    from fw.objects
    where object_id = p_object_id;*/
    v_act_time = true;
    if not coalesce(v_act_time, false) then
        perform fw.f_write_log('SERVICE', 'Now is not the time to start load object ' || p_object_id, v_location);
        return null;
    end if;
    v_extr_start = coalesce(p_start_date,c_minDate);
    v_extr_end   = coalesce(p_end_date,c_maxDate);
    -- look for existing load_id for load
    v_sql := 'select load_id, extraction_from, extraction_to from fw.load_info 
               where object_id = ' || p_object_id::text ||
             ' and load_status = ' || v_load_id_new_status::text || '
               and extraction_from >= '''|| v_extr_start||'''::timestamp and extraction_to <= '''||v_extr_end||'''::timestamp order by 2 desc limit 1';
    execute v_sql into v_load_id;

    if v_load_id is not null then
     perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'Find load_id for object ' || p_object_id, 
       p_location    := v_location,
       p_load_id     := v_load_id);
    else
     perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'No active load_id for object ' || p_object_id, 
       p_location    := v_location); 
    end if;
    perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'END get load_id for object ' || p_object_id||', load_id is: '||coalesce(v_load_id::text,'{empty}'), 
       p_location    := v_location); 
    return v_load_id;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_get_load_id(int8, date, date) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_get_load_id(int8, date, date) TO public;
GRANT ALL ON FUNCTION fw.f_get_load_id(int8, date, date) TO "admin";
