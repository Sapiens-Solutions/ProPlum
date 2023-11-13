CREATE OR REPLACE FUNCTION ${target_schema}.f_insert_table_sql(p_table_to text, p_sql text, p_truncate_tgt bool DEFAULT false)
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
    v_location text := '${target_schema}.f_insert_table_sql';
    v_table_to text;
    v_cnt int8;
    v_sql text;
BEGIN

    v_table_to = ${target_schema}.f_unify_name(p_name := p_table_to);
    --Log
    perform ${target_schema}.f_write_log(p_log_type := 'SERVICE', 
                         p_log_message := 'START Insert data into table '||v_table_to||' from sql '||p_sql, 
                         p_location    := v_location); --log function call
    
    if coalesce(p_truncate_tgt,false) is true then
     perform ${target_schema}.f_truncate_table(v_table_to);
    end if;
    --Insert
    EXECUTE 'INSERT INTO '||v_table_to||' '||p_sql;
    GET DIAGNOSTICS v_cnt = ROW_COUNT;
    raise notice '% rows inserted from sql: % into %',v_cnt,p_sql,v_table_to;
    --Log
    perform ${target_schema}.f_write_log(p_log_type := 'SERVICE', 
                         p_log_message := 'END Insert data into table '||v_table_to||' from sql '||p_sql||', '||v_cnt||' rows inserted', 
                         p_location    := v_location); --log function call
    return v_cnt;
    exception when others then 
     raise notice 'Insert data into table % from sql % finished with error: %',v_table_to, p_sql,sqlerrm;
     PERFORM ${target_schema}.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Insert data into table'||v_table_to||' from sql '||p_sql||' finished with error: '||SQLERRM, 
        p_location    := v_location);
     return null;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_insert_table_sql(text, text, bool) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_insert_table_sql(text, text, bool) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_insert_table_sql(text, text, bool) TO "${owner}";
