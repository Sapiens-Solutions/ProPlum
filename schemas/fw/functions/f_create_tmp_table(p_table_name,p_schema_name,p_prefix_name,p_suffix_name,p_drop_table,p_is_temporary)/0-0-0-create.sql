CREATE OR REPLACE FUNCTION fw.f_create_tmp_table(p_table_name text, p_schema_name text DEFAULT NULL::text, p_prefix_name text DEFAULT NULL::text, p_suffix_name text DEFAULT NULL::text, p_drop_table bool DEFAULT true, p_is_temporary bool DEFAULT false)
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
		
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*create temp table for template table*/
DECLARE
    v_location      text := 'fw.f_create_tmp_table';
    v_table_name    text;
    v_table_type    text;
    v_full_table_name text;
    v_tmp_t_name    text;
    v_storage_param text;
    v_sql           text;
    v_suffix_name   text;
    v_prefix_name   text;
    v_schema_name   text;
    v_dist_key      text;
    v_exists        bool;
    
BEGIN
    v_suffix_name = coalesce(p_suffix_name,'');
    v_prefix_name = coalesce(p_prefix_name,'');
    v_table_name  = fw.f_unify_name(p_name := p_table_name);
    v_full_table_name  = fw.f_unify_name(p_name := v_table_name); -- full table name
    v_schema_name = left(v_full_table_name,position('.' in v_full_table_name)-1); -- target table schema name
    v_schema_name = coalesce(p_schema_name,v_schema_name);
    v_table_name =  right(v_full_table_name,length(v_full_table_name) - POSITION('.' in v_full_table_name));-- table name wo schema
    if p_is_temporary then 
      v_tmp_t_name = v_prefix_name||v_table_name||v_suffix_name;
      v_table_type = 'TEMP';
    else 
      v_tmp_t_name = v_schema_name||'.'||v_prefix_name||v_table_name||v_suffix_name;
      v_table_type = '';
    end if;
    v_tmp_t_name   = fw.f_unify_name(p_name := v_tmp_t_name);
    v_storage_param = fw.f_get_table_attributes(p_table_name := v_full_table_name);
    v_dist_key = fw.f_get_distribution_key(p_table_name := v_full_table_name);
    
    perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START Creating temp table '||v_tmp_t_name||' for table '||v_full_table_name,
     p_location    := v_location); --log function call
    if v_full_table_name = v_tmp_t_name then 
     perform fw.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Names of source table '|| v_full_table_name|| ' and target table '||v_tmp_t_name||' are equal. Stop processing',
        p_location    := v_location); --log function call
     RAISE EXCEPTION 'Names of source table % and target table % are equal. Stop processing',v_full_table_name,v_tmp_t_name;
    end if; 
    if coalesce(p_drop_table,false) is true then
      v_sql := 
          'DROP TABLE IF EXISTS ' || v_tmp_t_name || ';'
          || 'CREATE '||v_table_type||' TABLE ' || v_tmp_t_name || ' (like ' || v_full_table_name || ') ' || v_storage_param||' '||v_dist_key||';';
    else
      v_exists = fw.f_table_exists(p_table_name := v_tmp_t_name);
      if v_exists is true then
       perform fw.f_truncate_table(p_table_name := v_tmp_t_name);
       v_sql = 'select 1';
      else
       v_sql := 
         'CREATE '||v_table_type||' TABLE ' || v_tmp_t_name || ' (like ' || v_full_table_name || ') ' || v_storage_param||' '||v_dist_key||';';
      end if;
    end if;
    perform fw.f_write_log(
     p_log_type    := 'DEBUG', 
     p_log_message := 'v_sql:{'||v_sql||'}',
     p_location    := v_location); --log function call
    EXECUTE v_sql;
    --permissions
    PERFORM fw.f_grant_select(
       p_trg_table_name := v_tmp_t_name,
       p_src_table      := v_full_table_name);
    
    perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'END Creating '||v_table_type||' table '||v_tmp_t_name||' for table '||v_full_table_name,
     p_location    := v_location); --log function call
    return v_tmp_t_name;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_create_tmp_table(text, text, text, text, bool, bool) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_create_tmp_table(text, text, text, text, bool, bool) TO public;
GRANT ALL ON FUNCTION fw.f_create_tmp_table(text, text, text, text, bool, bool) TO "admin";
