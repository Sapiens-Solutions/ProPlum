CREATE OR REPLACE FUNCTION ${target_schema}.f_grant_select(p_trg_table_name text, p_src_table text)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/* function changes table owner, also gets current SELECT privileges on base table and applies it to tmp table */

declare 
 v_location text := '${target_schema}.f_grant_select';
 v_result text;
 v_owner  text;
 v_src_table text;
 v_trg_table_name text;

begin
 perform ${target_schema}.f_write_log(p_log_type    := 'SERVICE', 
                         p_log_message := 'Start grant select on table '||p_trg_table_name||' from table '||p_src_table, 
                         p_location    := v_location); --log function call
 v_src_table      = ${target_schema}.f_unify_name(p_name := p_src_table);
 v_trg_table_name = ${target_schema}.f_unify_name(p_name := p_trg_table_name);
--change table owner to source table owner
 select tableowner into v_owner from pg_tables where schemaname||'.'||tablename = v_src_table;
 execute 'ALTER TABLE ' || v_trg_table_name || ' OWNER TO "'||v_owner||'";';

--get select privileges from src table and apply it to trg table
SELECT coalesce(STRING_AGG(REPLACE(' GRANT ALL ON TABLE '||r.table_schema||'.'||r.table_name||' TO "'||r.grantee||'"', lower(v_src_table), lower(v_trg_table_name)),';'),'') into v_result
			FROM information_schema.role_table_grants r
			LEFT JOIN pg_catalog.pg_partitions p ON r.table_name=p.partitiontablename
			WHERE p.partitiontablename IS NULL
			  AND r.table_schema||'.'||r.table_name=lower(v_src_table) AND r.privilege_type='SELECT';
 EXECUTE v_result;
 perform ${target_schema}.f_write_log(p_log_type    := 'SERVICE', 
                         p_log_message := 'End grant select on table '||p_trg_table_name||' from table '||p_src_table, 
                         p_location    := v_location); --log function call
end;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_grant_select(text, text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_grant_select(text, text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_grant_select(text, text) TO "${owner}";
