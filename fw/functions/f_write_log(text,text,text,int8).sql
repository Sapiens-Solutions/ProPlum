CREATE OR REPLACE FUNCTION ${target_schema}.f_write_log(p_log_type text, p_log_message text, p_location text, p_load_id int8 DEFAULT NULL::bigint)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function logs*/
DECLARE
  v_log_type text;
  v_debug      boolean := true;
  v_notice     boolean := true;
  v_log_debug  boolean := true;
  v_log_message text;
  v_sql text;
  v_res	text;
  v_load_id int8;
  v_host text;
  v_server text;
  v_location text;
BEGIN

  --Check message type
  v_log_type = upper(p_log_type);
  v_location = lower(p_location);
  v_server = lower(${target_schema}.f_get_constant('c_log_fdw_server'));
  if p_load_id is null then
    v_res = ${target_schema}.f_get_session_param('${target_schema}.load_id');
    if v_res is null or v_res = '' then
      v_load_id = 0;
    else 
     v_load_id = v_res::int8;
    end if;
  else
    v_load_id = p_load_id;
  end if;
  
  IF v_log_type not in ('ERROR', 'INFO', 'WARN', 'DEBUG','SERVICE') THEN
    RAISE EXCEPTION 'Illegal log type! Use one of: INFO, WARN, ERROR, DEBUG, SERVICE';
  END IF;

  IF v_log_type = 'DEBUG' AND  v_debug = false THEN
    RETURN;
  END IF;

  IF v_notice THEN
    RAISE NOTICE '%: %: <%> Location[%] Load Id[%]', clock_timestamp(), v_log_type, p_log_message, v_location, coalesce(v_load_id::text,'empty');
  END IF;

  IF v_log_type = 'DEBUG' AND v_log_debug = false THEN
    RETURN;
  END IF;
  v_log_message := replace(p_log_message, '''', '''''');
  
  v_sql := 'INSERT INTO ${target_schema}.logs(log_id, load_id, log_type, log_msg, log_location, is_error,log_timestamp,log_user) 
            VALUES ( '||nextval('${target_schema}.log_id_seq')||' , '||coalesce(v_load_id, 0)||', '''||v_log_type||''', '||coalesce(''''||v_log_message||'''','''empty''')||', '||coalesce(''''||v_location||'''','null')||', '||case when v_log_type='ERROR' THEN true else false end||',current_timestamp,current_user);';
   --dblink for fdw server
  v_res := dblink(v_server,v_sql);
 
  END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_write_log(text, text, text, int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_write_log(text, text, text, int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_write_log(text, text, text, int8) TO "${owner}";
