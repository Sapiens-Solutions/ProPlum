CREATE OR REPLACE FUNCTION ${target_schema}.f_create_ext_table(p_table_name text, p_load_id int8, p_load_method text DEFAULT NULL::text, p_connect_string text DEFAULT NULL::text, p_schema_name text DEFAULT NULL::text, p_prefix text DEFAULT NULL::text, p_suffix text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	
	
	
	
	
	
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
	/*create external table from template table*/
DECLARE
  v_location     text := '${target_schema}.f_create_ext_table';
  v_ext_t_name   text;
  v_table_name   text;
  v_load_method  text;
  v_sql          text;
  v_sql_conn     text;
  v_error text;
  v_suffix   text;
  v_prefix   text;
  v_schema_name   text;
  v_full_table_name text;
  v_columns text;
  v_date_type text;
  v_ts_type   text;
  v_object_id int8;
BEGIN

  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Start create external table for '||p_table_name, 
     p_location    := v_location); --log function call
  v_suffix = coalesce(p_suffix,'');
  v_prefix = coalesce(p_prefix,'');
  v_table_name  = ${target_schema}.f_unify_name(p_name := p_table_name);
  v_full_table_name  = ${target_schema}.f_unify_name(p_name := v_table_name); -- full table name
  v_schema_name = left(v_full_table_name,position('.' in v_full_table_name)-1); -- target table schema name
  v_schema_name = coalesce(p_schema_name,v_schema_name);
  v_table_name =  right(v_full_table_name,length(v_full_table_name) - POSITION('.' in v_full_table_name));-- table name wo schema
  v_ext_t_name := v_schema_name||'.'||v_prefix||v_table_name||v_suffix;
  if not ${target_schema}.f_table_exists(v_full_table_name) then 
    v_error := 'No table with name '||v_full_table_name;
    PERFORM ${target_schema}.f_write_log(
        p_log_type := 'ERROR', 
        p_log_message := v_error, 
        p_location := v_location);
    RAISE EXCEPTION '%',v_error;
  end if;
  perform ${target_schema}.f_write_log(
     p_log_type    := 'DEBUG', 
     p_log_message := 'Create external table for '||v_full_table_name|| ' with parameters: v_ext_t_name: '||v_ext_t_name, 
     p_location    := v_location); --log function call
  
  select ob.object_id, coalesce(p_load_method, ob.load_method), coalesce(src_date_type,'date'), coalesce(src_ts_type,'timestamp')
   from  ${target_schema}.objects ob 
    join ${target_schema}.load_info li on ob.object_id = li.object_id 
  where li.load_id = p_load_id
  into v_object_id, v_load_method, v_date_type, v_ts_type;
  IF v_load_method IS null then 
        v_error := 'Unable to create external table with empty load method';
        PERFORM ${target_schema}.f_write_log(
           p_log_type := 'ERROR', 
           p_log_message := v_error, 
           p_location := v_location);
        RAISE EXCEPTION '%',v_error;
  END IF;
 
  v_load_method := ${target_schema}.f_unify_name(p_name := v_load_method);
  perform ${target_schema}.f_write_log(
     p_log_type    := 'DEBUG', 
     p_log_message := 'Variable: v_full_table_name:{'||v_full_table_name||'}', 
     p_location    := v_location); --log function call
  perform ${target_schema}.f_write_log(
     p_log_type    := 'DEBUG', 
     p_log_message := 'Variable: v_ext_t_name:{'||v_ext_t_name||'}', 
     p_location    := v_location); --log function call
  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START Creating external table '||v_ext_t_name||' for table '||v_full_table_name, 
     p_location    := v_location); --log function call
  -- Recreate table
  v_sql :=  'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_t_name; 
  perform ${target_schema}.f_write_log(
     p_log_type    := 'DEBUG', 
     p_log_message := 'Variable: v_sql:{'||v_sql||'}', 
     p_location    := v_location); --log function call
  EXECUTE v_sql;
  if coalesce(p_connect_string,'') = '' then
  case  v_load_method 
   when 'gpfdist' then 
    v_sql_conn = ${target_schema}.f_get_connection_string(p_load_id := p_load_id);
   when 'pxf' then
    v_sql_conn = ${target_schema}.f_get_connection_string(p_load_id := p_load_id);
   when 'odata' then -------------------new
   	v_sql_conn = ${target_schema}.f_get_connection_string(p_load_id := p_load_id); -----------new
   when 'python' then 
    -- no need to create external table for python load method
     perform ${target_schema}.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'END Creating external table for table '||v_full_table_name|| '. No need to create external table for python load method', 
        p_location    := v_location); --log function call
     return '';
   when 'dblink' then 
    -- no need to create external table for dblink load method
     perform ${target_schema}.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'END Creating external table for table '||v_full_table_name|| '. No need to create external table for dblink load method', 
        p_location    := v_location); --log function call
     return '';
   else
        v_error := 'Unknown load method '|| v_load_method;
        perform ${target_schema}.f_write_log(
           p_log_type    := 'ERROR', 
           p_log_message := v_error, 
           p_location    := v_location); --log function call
        RAISE EXCEPTION '%',v_error;
   end CASE;
 else
  v_sql_conn = p_connect_string;
 end if;

 -- get columns from template table
  select
			string_agg('"'||coalesce(ob.column_name_mapping->>c.column_name,c.column_name)||'"'||' '||
		    case 
				when data_type = 'time' or data_type = 'time without time zone'  then 'timestamp' 
				when data_type = 'character' or data_type = 'character varying'  then 'text'--coalesce(data_type||'('||character_maximum_length||')',data_type)
				when data_type = 'interval'   then 'text'
				when data_type = 'date'       then v_date_type
				when data_type ~ 'timestamp%' then v_ts_type
				else data_type 
		   end,','  order by c.sorting_column
   ) from
   (
   select *, case when 1=0 /*v_load_method = 'gpfdist' or v_load_method = 'odata'*/ then alphabetical_position else ordinal_position end as sorting_column 
   from
   	(select *, row_number() over( partition by table_schema, table_name order by column_name) as alphabetical_position from information_schema.columns ) ccc
   ) c, -----------------------------
   ${target_schema}.objects ob   
   where c.table_schema||'.'||c.table_name = v_full_table_name
     and ob.object_id = v_object_id
   into v_columns;
  v_columns = replace(v_columns,'""','"');
  v_sql :=  ' CREATE READABLE EXTERNAL TABLE '  || v_ext_t_name ||' ('|| v_columns || ') ' ||v_sql_conn;
  raise notice 'v_ext_t_name: [%], v_columns: [%], v_sql_conn: [%]',v_ext_t_name,v_columns,v_sql_conn;
  raise notice 'v_sql with columns: %',v_sql;
  
  perform ${target_schema}.f_write_log(
     p_log_type    := 'DEBUG', 
     p_log_message := 'Variable: v_sql:{'||coalesce(v_sql,'empty')||'}', 
     p_location    := v_location); --log function call
  EXECUTE v_sql;
  -- set permissions
  --perform ${target_schema}.f_grant_select(v_ext_t_name,v_full_table_name);

  perform ${target_schema}.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'END Creating external table '||v_ext_t_name||' for table '||v_full_table_name, 
     p_location    := v_location); --log function call
  return v_ext_t_name;
END










$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_create_ext_table(text, int8, text, text, text, text, text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_create_ext_table(text, int8, text, text, text, text, text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_create_ext_table(text, int8, text, text, text, text, text) TO "${owner}";
