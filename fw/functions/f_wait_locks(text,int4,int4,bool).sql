CREATE OR REPLACE FUNCTION ${target_schema}.f_wait_locks(p_table_name text, p_repeat_interval int4, p_repeat_count int4, p_terminate_lock bool DEFAULT false)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function wait until no locks on table*/
declare
    v_location     text    := '${target_schema}.f_wait_locks';
    v_repeat_count integer := 0; --count of repeats
begin
    perform ${target_schema}.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'START checking locks on the table ' || p_table_name, 
       p_location    := v_location);
    if p_terminate_lock then 
     perform ${target_schema}.f_terminate_lock(p_table_name := p_table_name);
    else
    while exists(
            select 1
            from ${target_schema}.f_get_locks() l, --get count of locks on the table or its partitions
                 pg_catalog.pg_database d,
                 ${target_schema}.f_stat_activity() a
            where l.locktype = 'relation'
              and d.oid = l.database
              and d.datname = current_database()
              and l.relation in (
                select (partitionschemaname || '.' || partitiontablename)::regclass
                from pg_partitions p
                where p.schemaname = split_part(p_table_name, '.', 1)
                  and p.tablename = split_part(p_table_name, '.', 2)
                union
                select p_table_name::regclass
            )
              and l.mppsessionid = a.sess_id
              and a.pid <> pg_backend_pid()
        )
        loop
            perform pg_sleep(p_repeat_interval);--interval in seconds
            v_repeat_count = v_repeat_count + 1;

            perform ${target_schema}.f_write_log(
               p_log_type    := 'SERVICE',
               p_log_message := 'CONTINUE checking locks on the table ' || p_table_name || '. Step number: ' ||v_repeat_count::text, 
               p_location    := v_location);

            if v_repeat_count = p_repeat_count then
                PERFORM ${target_schema}.f_write_log(
                   p_log_type    := 'ERROR', 
                   p_log_message := 'Number of steps reached the limit ' || p_repeat_count::text, 
                   p_location    := v_location);
                RAISE exception 'Number of steps reached its limit';
            end if;
        end loop;
    end if;
END;




$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_wait_locks(text, int4, int4, bool) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_wait_locks(text, int4, int4, bool) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_wait_locks(text, int4, int4, bool) TO "${owner}";
