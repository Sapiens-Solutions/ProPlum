CREATE OR REPLACE FUNCTION ${target_schema}.f_insert_table(p_table_from text, p_table_to text, p_where text DEFAULT NULL::text, p_truncate_tgt bool DEFAULT false)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function insert data from one table to another*/
DECLARE
    v_location text := '${target_schema}.f_insert_table';
    v_table_from text;
    v_table_to text;
    v_where text;
    v_cnt int8;
BEGIN
    v_table_from = ${target_schema}.f_unify_name(p_name := p_table_from);
    v_table_to = ${target_schema}.f_unify_name(p_name := p_table_to);
    v_where = coalesce(p_where,' 1 = 1 ');
    --Log
    perform ${target_schema}.f_write_log(p_log_type := 'SERVICE', 
                         p_log_message := 'START Insert data from table '||v_table_from||' to '||v_table_to || ' with condition: '||v_where, 
                         p_location    := v_location); --log function call
    
    if coalesce(p_truncate_tgt,false) is true then
     perform ${target_schema}.f_truncate_table(v_table_to);
    end if;
    --Insert
    EXECUTE 'INSERT INTO '||v_table_to||' SELECT * FROM '||v_table_from || ' where '||v_where;
    GET DIAGNOSTICS v_cnt = ROW_COUNT;
    raise notice '% rows inserted from % into %',v_cnt, v_table_from,v_table_to;
    --Log
    perform ${target_schema}.f_write_log(p_log_type := 'SERVICE', 
                         p_log_message := 'END Insert data from table '||v_table_from||' to '||v_table_to||', '||v_cnt||' rows inserted', 
                         p_location    := v_location); --log function call
    return v_cnt;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_insert_table(text, text, text, bool) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_insert_table(text, text, text, bool) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_insert_table(text, text, text, bool) TO "${owner}";
