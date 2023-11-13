CREATE OR REPLACE FUNCTION ${target_schema}.f_terminate_lock(p_table_name text)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function terminate lock processes on table*/
DECLARE
  v_location text := '${target_schema}.f_terminate_lock';
  v_res bool;
  v_table_name text;
  rec record;
begin
   
   v_table_name = ${target_schema}.f_unify_name(p_table_name);
   for rec in (select distinct a.pid
    from ${target_schema}.f_get_locks() l, --get count of locks on the table or its partitions
         pg_catalog.pg_database d,
         ${target_schema}.f_stat_activity() a
    where l.locktype = 'relation'
      and d.oid = l.database
      and d.datname = current_database()
      and l.relation in (
        select (partitionschemaname || '.' || partitiontablename)::regclass
        from pg_partitions p
        where p.schemaname = split_part(v_table_name, '.', 1)
          and p.tablename = split_part(v_table_name, '.', 2)
        union
        select v_table_name::regclass
    )
      and l.mppsessionid = a.sess_id
      and a.pid <> pg_backend_pid())
     loop
      v_res = ${target_schema}.f_terminate_backend(rec.pid);
      raise notice 'Terminate process with pid %', rec.pid;
     END LOOP;
    perform ${target_schema}.f_write_log( 
     p_log_type    := 'SERVICE', 
     p_log_message := 'Terminate locks on object '||v_table_name, 
     p_location    := v_location); --log function call
   return v_res;
   exception when others then 
    perform ${target_schema}.f_write_log(
      p_log_type    := 'ERROR', 
      p_log_message := 'Terminate locks on object '||v_table_name||' finished with error: '||sqlerrm , 
      p_location    := v_location); --log function call
    return false;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_terminate_lock(text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_terminate_lock(text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_terminate_lock(text) TO "${owner}";
