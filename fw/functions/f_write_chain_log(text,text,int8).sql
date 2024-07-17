CREATE OR REPLACE FUNCTION ${target_schema}.f_write_chain_log(p_log_type text, p_log_message text, p_instance_id int8)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$ 
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2024*/
    /*Write chains log*/
DECLARE
  v_log_type text;
  v_debug      boolean := true;
  v_notice     boolean := true;
  v_log_debug  boolean := true;
  v_log_message text;
  v_sql text;
  v_res	text;
  v_server text;
BEGIN

  --Check message type
  v_log_type = upper(p_log_type);
  v_server = lower(${target_schema}.f_get_constant('c_log_fdw_server'));
  IF v_log_type not in ('ERROR', 'INFO', 'WARN', 'DEBUG','SERVICE') THEN
    RAISE EXCEPTION 'Illegal log type! Use one of: INFO, WARN, ERROR, DEBUG, SERVICE';
  END IF;

  IF v_log_type = 'DEBUG' AND  v_debug = false THEN
    RETURN;
  END IF;

  IF v_notice THEN
    RAISE NOTICE '%: %: <%> instance_id[%]', clock_timestamp(), v_log_type, p_log_message, coalesce(p_instance_id::text,'empty');
  END IF;
  IF v_log_type = 'DEBUG' AND v_log_debug = false THEN
    RETURN;
  END IF;
  v_log_message := replace(p_log_message, '''', '''''');
  
  v_sql := 'INSERT INTO ${target_schema}.chains_log(log_id, instance_id, log_timestamp, log_type, log_msg) 
            VALUES ( '||nextval('${target_schema}.chain_log_id_seq')||' , '||coalesce(p_instance_id, 0)||', current_timestamp,'''||v_log_type||''', '||coalesce(''''||v_log_message||'''','''empty''')||');';
  --dblink for fdw server
  v_res := dblink(v_server,v_sql);
 END;
$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_write_chain_log(text, text, int8) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_write_chain_log(text, text, int8) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_write_chain_log(text, text, int8) TO "${owner}";