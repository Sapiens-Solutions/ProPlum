CREATE OR REPLACE FUNCTION ${target_schema}.f_gen_instance_id(p_chain_name text, p_load_from timestamp DEFAULT NULL::timestamp without time zone, p_load_to timestamp DEFAULT NULL::timestamp without time zone)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2024*/
/*Function generates instance_id for chain*/
DECLARE
    v_location   text := '${target_schema}.f_gen_instance_id';
    v_chain_name text;
    v_start_date timestamp;
    v_end_date   timestamp;
    v_error      text;
    v_sql        text;
    v_instance_id int8;
    c_new_status int := 1;

BEGIN
    perform ${target_schema}.f_write_chain_log(
       p_log_type := 'SERVICE', 
       p_log_message := 'START Generate instance_id for chain '||p_chain_name,
       p_instance_id := null); --log function call
    select chain_name from ${target_schema}.chains 
     where chain_name = p_chain_name
     into v_chain_name;
    if v_chain_name is null then
     RAISE EXCEPTION 'No chain with name %',p_chain_name;
     return null;
    end if;
    v_instance_id = nextval('${target_schema}.instance_id_seq');
    v_sql := 'insert into ${target_schema}.chains_info
              (instance_id, chain_name, load_from, load_to, status, chain_start) values (' ||
              v_instance_id::text || ', ''' || 
              p_chain_name|| ''', '||
              decode(p_load_from is null,true,'null::timestamp,',''''||p_load_from||''',')||
              decode(p_load_to is null,true,'null::timestamp,',''''||p_load_to||''',')||
              c_new_status::text || ',''' || 
              current_timestamp||''');';
       execute v_sql;
       perform ${target_schema}.f_write_chain_log(
         p_log_type := 'SERVICE', 
         p_log_message := 'Generate instance_id for chain '||p_chain_name ||', instance_id = '||coalesce(v_instance_id::text,'empty'),
         p_instance_id := v_instance_id); --log function call
    if v_instance_id is null then
      perform ${target_schema}.f_write_chain_log(
         p_log_type    := 'ERROR',
         p_log_message := 'Unable to generate instance_id for chain '||p_chain_name,
         p_instance_id := null); --log function call
    end if;
    RETURN v_instance_id;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_gen_instance_id(text, timestamp, timestamp) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_gen_instance_id(text, timestamp, timestamp) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_gen_instance_id(text, timestamp, timestamp) TO "${owner}";
