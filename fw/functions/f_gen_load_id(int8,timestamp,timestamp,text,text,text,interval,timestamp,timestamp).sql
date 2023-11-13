CREATE OR REPLACE FUNCTION ${target_schema}.f_gen_load_id(p_object_id int8, p_start_extr timestamp DEFAULT NULL::timestamp without time zone, p_end_extr timestamp DEFAULT NULL::timestamp without time zone, p_extraction_type text DEFAULT NULL::text, p_load_type text DEFAULT NULL::text, p_delta_mode text DEFAULT NULL::text, p_load_interval interval DEFAULT NULL::interval, p_start_load timestamp DEFAULT NULL::timestamp without time zone, p_end_load timestamp DEFAULT NULL::timestamp without time zone)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
		
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function generates load_id for object*/
DECLARE
    v_location           text := '${target_schema}.f_gen_load_id';
    v_object_id          int8;
    v_start_date         timestamp;
    v_end_date           timestamp;
    v_begindate          timestamp;
    v_lastdate           timestamp;
    v_load_id            int8;
    v_load_type          text;
    v_extraction_type    text;
    v_load_method        text;
    v_delta_mode         text;
    v_partition_type     text;
    v_error              text;
    v_load_interval      interval;
    v_iterDate           timestamp;
    c_load_id_new_status int  := 1;
    c_load_id_succ_status int  := 3;
    c_minDate            date := '1900-01-01';
    c_maxDate            date := '9999-12-31';
    c_startDate          timestamp := '2000-01-01';
    v_safetyperiod       interval;
    v_startupdate        date;
    v_endupdate          date;
    v_add_days           interval;
    i                    int;
    k                    int;
    v_sql                text;
BEGIN

    perform ${target_schema}.f_write_log(
       p_log_type := 'SERVICE', 
       p_log_message := 'START Generate load_id for object '||p_object_id, 
       p_location    := v_location); --log function call

   v_load_id = ${target_schema}.f_get_load_id( --find existing load_id
      p_object_id  := p_object_id, 
      p_start_date := p_start_extr::date, 
      p_end_date   := p_end_extr::date);
     
   if v_load_id is not null then
    return v_load_id;
   end if;
    -- get object params
    select object_id,
           coalesce(
           case 
	        when coalesce(p_extraction_type, extraction_type) = 'PARTITION'
            then load_start_date
            else delta_start_date
           end, 
           case coalesce(p_delta_mode, delta_mode)
             when 'DELTA' then c_minDate
             when 'ITER'  then c_startDate 
           end), 
           coalesce(p_delta_mode, delta_mode),
           coalesce(p_extraction_type, extraction_type),
           coalesce(p_load_type, load_type),
           '1 day'::interval 
    into v_object_id, v_begindate, v_delta_mode, v_extraction_type, v_load_type, v_add_days
    from ${target_schema}.objects ob
    where object_id = p_object_id;

    perform ${target_schema}.f_write_log(
       p_log_type    := 'DEBUG',
       p_log_message := 'Variables: v_object_id:{' || v_object_id || '}, v_beginDate:{' || v_begindate || '}, v_delta_mode:{' ||v_delta_mode || '}, v_extraction_type:{' || v_extraction_type || ', v_load_type:{' || v_load_type || '}', 
       p_location    := v_location);

    -- no such object
    if v_object_id is null then
        v_error := 'Could not find object '||p_object_id||' for generating load_id';
        perform ${target_schema}.f_write_log(
           p_log_type    := 'ERROR', 
           p_log_message := 'Error while generating load_id: ' || v_error, 
           p_location    := v_location);
        raise exception '%', v_error;
    end if;

    -- get last load date
    v_sql := 'select coalesce(' || coalesce('''' || p_start_extr::text || '''', 'null') ||
             '::timestamp, max(extraction_to), ' || coalesce('''' || v_begindate::text || '''', 'null') ||
             '::timestamp) from ${target_schema}.load_info
             where object_id = ' || v_object_id::text || 
             ' and (extraction_to::date < current_date + ''' ||v_add_days::text || '''::interval)
             and load_status = '||c_load_id_succ_status::text;
    perform ${target_schema}.f_write_log(
       p_log_type    := 'DEBUG',
       p_log_message := 'Get last load date with sql: {' || v_sql || '}', 
       p_location    := v_location);

    execute v_sql into v_lastdate;

    perform ${target_schema}.f_write_log(
       p_log_type    := 'DEBUG',
       p_log_message := 'Last load date for object '||v_object_id||' is: '|| v_lastdate, 
       p_location    := v_location);
    select 
           coalesce(p_load_interval,load_interval,'0 days'::interval),
           case v_extraction_type 
            when 'PARTITION' then coalesce(bdate_safety_period, '0 days'::interval)
            else coalesce(delta_safety_period, '0 days'::interval)
           end,
           coalesce(p_start_extr,v_lastdate)::timestamp,
           coalesce(p_end_extr, current_timestamp)::timestamp,
           load_method
    into v_load_interval, v_safetyperiod,v_start_date,v_end_date,v_load_method
     from ${target_schema}.objects ob
    where ob.object_id = p_object_id;
  -- prepare start and end dates for load_load_interval dates
        IF v_load_interval::text like '%month%' or v_load_interval::text like '%mon%' THEN
            v_start_date := DATE_TRUNC('month', v_start_date);
            v_end_date := DATE_TRUNC('month', v_end_date) + v_load_interval;
        ELSIF v_load_interval::text like '%day%' THEN
            v_start_date := DATE_TRUNC('day', v_start_date);
            v_end_date := DATE_TRUNC('day', v_end_date) + '1 day'::interval; 
        ELSIF v_load_interval::text like '%year%' THEN
            v_start_date := DATE_TRUNC('year', v_start_date);
            v_end_date := DATE_TRUNC('year', v_end_date) + v_load_interval;
        ELSIF v_load_interval::text like '%hour%' THEN
            v_start_date := DATE_TRUNC('hour', v_start_date);
            v_end_date   := DATE_TRUNC('hour', v_end_date) + v_load_interval;
        ELSE
            v_start_date := v_start_date;
            v_end_date := v_end_date;
        END IF;
    perform ${target_schema}.f_write_log(
       p_log_type    := 'DEBUG', 
       p_log_message := 'Start date: {' || coalesce(v_start_date,c_minDate) || '}, end date: {' || coalesce(v_end_date,c_maxDate) || '}',
       p_location    := v_location);

    --create load_id for each template_type
    IF v_delta_mode = 'FULL' THEN
        --check if load_id exists
         IF NOT ${target_schema}.f_load_id_exists(p_object_id:=p_object_id, p_start_date:=c_minDate, p_end_date:=c_maxDate) THEN
            v_load_id = nextval('${target_schema}.load_id_seq');
            v_sql := 'insert into ${target_schema}.load_info
                     (load_id, extraction_type, load_type, object_id, load_status, extraction_from, extraction_to,load_method) values (' ||
                     v_load_id::text || ', ''' || 
                     v_extraction_type|| ''', '''||
                     v_load_type || ''', ' || 
                     p_object_id::text || ', ' ||
                     c_load_id_new_status::text || ', ''' || 
                     coalesce(v_start_date::text,c_minDate::text) || '''::timestamp, ''' ||
                     coalesce(v_end_date::text,c_maxDate::text) || '''::timestamp,'''||
                     v_load_method||'''::text);';
            execute v_sql;
            perform ${target_schema}.f_write_log(
               p_log_type    := 'SERVICE',
               p_log_message := 'Created load_id for object' || p_object_id, 
               p_location    := v_location, 
               p_load_id     := v_load_id);
        END IF;
    ELSIF v_delta_mode = 'DELTA' THEN
        IF v_extraction_type = 'DELTA' THEN
            --check if load_id exists
            IF NOT ${target_schema}.f_load_id_exists(p_object_id:=p_object_id, p_start_date:= (v_start_date - v_safetyperiod),p_end_date:=v_end_date) THEN
                v_load_id = nextval('${target_schema}.load_id_seq');
                v_sql := 'insert into ${target_schema}.load_info
						 (load_id, extraction_type, load_type, object_id, load_status, extraction_from, extraction_to, load_method) values (' ||
                         v_load_id::text || ', ''' || 
                         v_extraction_type|| ''', '''||
                         v_load_type || ''', ' || 
                         p_object_id::text || ', ' ||
                         c_load_id_new_status::text || ', ''' ||
                         (v_start_date - v_safetyperiod)::text || '''::timestamp, ''' ||
                         (v_end_date)::text || '''::timestamp,'''||
                         v_load_method||'''::text);';
                raise notice 'DEBUG: insert into load_info with sql v_sql: {%}',v_sql;
                execute v_sql;
                perform ${target_schema}.f_write_log(
                   p_log_type    := 'SERVICE',
                   p_log_message := 'Created load_id for object ' || p_object_id, 
                   p_location    := v_location, 
                   p_load_id     := v_load_id);
            END IF;
        ELSE
            --check if load_id exists
            IF NOT ${target_schema}.f_load_id_exists(p_object_id:=p_object_id, p_start_date:=v_start_date, p_end_date:=v_end_date) THEN
                v_load_id = nextval('${target_schema}.load_id_seq');
                v_sql := 'insert into ${target_schema}.load_info
                         (load_id, extraction_type, load_type, object_id, load_status, extraction_from, extraction_to, load_method) values (' ||
                         v_load_id::text || ', ''' || 
                         v_extraction_type|| ''', '''||
                         v_load_type || ''', ' || 
                         p_object_id::text || ', ' ||
                         c_load_id_new_status::text || ', ''' || 
                         (v_start_date - v_safetyperiod)::text ||'''::timestamp, ''' || 
                         v_end_date::text || '''::timestamp,'''||
                         v_load_method||'''::text);';
                raise notice 'DEBUG: insert into load_info with sql v_sql: {%}',v_sql;        
                execute v_sql;
                perform ${target_schema}.f_write_log(
                   p_log_type    := 'SERVICE',
                   p_log_message := 'Created load_id for object' || p_object_id, 
                   p_location    := v_location, 
                   p_load_id     := v_load_id);
            END IF;
        END IF;
-- load template - iter, created range of load_id for period
    ELSIF v_delta_mode = 'ITER' THEN
        IF (v_load_interval::text like '%month%' or v_load_interval::text like '%mon%' or
            v_load_interval::text like '%year%'  or v_load_interval::text like '%day%' or 
            v_load_interval::text like '%hour%') THEN
            LOOP
                v_iterDate = v_start_date + v_load_interval;
    
                PERFORM ${target_schema}.f_write_log(
                   p_log_type    := 'DEBUG', 
                   p_log_message := 'v_iterDate:{' || v_iterDate || '}', 
                   p_location    := v_location);
                --loop till the end of range
                EXIT WHEN ((v_iterDate >= v_end_date) AND (v_load_interval = '1 day'::interval)) OR
                          ((v_iterDate > v_end_date) AND (v_load_interval <> '1 day'::interval));

                --check load_id already exists
                IF NOT ${target_schema}.f_load_id_exists(p_object_id:=p_object_id, p_start_date:= (v_start_date - v_safetyperiod), p_end_date:=v_iterDate) THEN
                      v_load_id = nextval('${target_schema}.load_id_seq');
                      v_sql := 'insert into ${target_schema}.load_info
                               (load_id, extraction_type, load_type, object_id, load_status, extraction_from, extraction_to, load_method) values (' ||
                               v_load_id::text || ', ''' || 
                               v_extraction_type|| ''', '''||
                               v_load_type || ''', ' || 
                               p_object_id::text || ', ' ||
                               c_load_id_new_status::text ||', '''|| 
                               (v_start_date - v_safetyperiod)::text ||'''::timestamp, ''' || 
                               v_iterDate::text || '''::timestamp, '''||
                               v_load_method||'''::text);';
                      RAISE NOTICE 'sql string for insert [%]',v_sql;
                      execute v_sql;
                      perform ${target_schema}.f_write_log(
                         p_log_type    := 'SERVICE',
                         p_log_message := 'Created load_id '||v_load_id||' with interval (' || v_start_date || '-' || v_iterDate||' for object ' || p_object_id, 
                         p_location    := v_location, 
                         p_load_id     := v_load_id);
                END IF;
                v_start_date := v_iterDate;
            END LOOP;
        ELSIF v_load_interval is null then
            perform ${target_schema}.f_write_log(
               p_log_type    := 'ERROR',
               p_log_message := 'load_interval could not be null for ITER object '||p_object_id, 
               p_location    := v_location);
            RAISE EXCEPTION 'load_interval could not be null for ITER object %',p_object_id;
        END IF;
    ELSE
       v_error := 'Illegal object delta_mode ';
       perform ${target_schema}.f_write_log(
          p_log_type    := 'ERROR',
          p_log_message := v_error || v_delta_mode || ' for object ' || p_object_id,
          p_location    := v_location);
        RAISE EXCEPTION '% % for object %',v_error, v_delta_mode,p_object_id;
    END IF;
    perform ${target_schema}.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'END Generate load_id for object ' ||p_object_id, 
       p_location    := v_location);
    if v_load_id is null then
      perform ${target_schema}.f_write_log(
         p_log_type    := 'ERROR',
         p_log_message := 'Unable to generate load_id for object '||p_object_id,
         p_location    := v_location);
    end if;
    RETURN v_load_id;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_gen_load_id(int8, timestamp, timestamp, text, text, text, interval, timestamp, timestamp) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_gen_load_id(int8, timestamp, timestamp, text, text, text, interval, timestamp, timestamp) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_gen_load_id(int8, timestamp, timestamp, text, text, text, interval, timestamp, timestamp) TO "${owner}";
