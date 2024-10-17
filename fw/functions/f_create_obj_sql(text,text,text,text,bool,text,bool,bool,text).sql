CREATE OR REPLACE FUNCTION ${target_schema}.f_create_obj_sql(p_schema_name text, p_obj_name text, p_sql_text text, p_grants_template text DEFAULT NULL::text, p_mat_flg bool DEFAULT true, p_storage_opt text DEFAULT NULL::text, p_analyze_flg bool DEFAULT true, p_temporary bool DEFAULT false, p_distr_cls text DEFAULT NULL::text)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*create object from sql */

declare 
    v_schema_name text;
    v_obj_name text;
    v_obj_name_full text;
    v_cnt int8;
    v_cr_obj text;
    v_cr_obj_type text;
    v_sql_text text;
    v_sql_in text:='('||p_sql_text||')';
    v_location text := '${target_schema}.f_create_obj_sql';
begin
    v_cr_obj_type:=decode(p_mat_flg,true,decode(p_temporary,true,'temp table ','table '),'view ') ;
    v_schema_name = coalesce(lower(p_schema_name),'public');
    v_obj_name = lower(p_obj_name);
    v_obj_name_full = decode(p_temporary,true,'',v_schema_name||'.')||v_obj_name;
    PERFORM ${target_schema}.f_write_log(
      p_log_type    := 'SERVICE', 
      p_log_message := 'START Create '||v_cr_obj_type||' '||v_obj_name_full||' with sql: '||p_sql_text, 
      p_location    := v_location);
    /*1.1.Check object already exists */
    /*1.2.if table or view already exists*/
    select to_regclass(v_obj_name_full::cstring) into v_cr_obj;
    if v_cr_obj is not null then
      select max(tablename) into v_cr_obj from pg_catalog.pg_tables pt  where schemaname=v_schema_name and tablename=v_obj_name;
      v_sql_text:='drop '||decode(v_cr_obj,null,'view','table')||' if exists ' ||v_obj_name_full||' cascade';
      --Log
      PERFORM ${target_schema}.f_write_log(        
        p_log_type    := 'SERVICE',
        p_log_message := 'Drop '||v_obj_name_full||' with sql: '||v_sql_text,
        p_location    := v_location);
      execute v_sql_text;
    end if;
  /*2. Create object with sql */
  if p_mat_flg is true then
    v_sql_text:='create '||v_cr_obj_type||' '||v_obj_name_full||
                 coalesce(' '||p_storage_opt,' WITH (appendonly=true,orientation=column,compresstype=zstd,compresslevel=1)')||
               ' as select * from '||v_sql_in||' tb '||
               ' distributed '||case when p_distr_cls is null then 'randomly' when  lower(p_distr_cls) in ('randomly','replicated') then p_distr_cls else 'by('||p_distr_cls||')' end ;
  else
    v_sql_text := 'create or replace view '||v_obj_name_full||' as select * from '||v_sql_in||' tmp';
  end if;
  
  PERFORM ${target_schema}.f_write_log(
    p_log_type    := 'SERVICE',
    p_log_message := 'Create '||v_cr_obj_type||' '||v_obj_name_full||' with sql: '||v_sql_text,
    p_location    := v_location);
   execute v_sql_text;
   
   GET DIAGNOSTICS v_cnt = ROW_COUNT;
   
   if p_mat_flg is true then
    PERFORM ${target_schema}.f_write_log(        
      p_log_type    := 'SERVICE',  
      p_log_message := 'Table '||v_obj_name_full||' was created successfully. '||v_cnt||' rows appended',
      p_location    := v_location);
   else 
    PERFORM ${target_schema}.f_write_log(        
      p_log_type    := 'SERVICE',  
      p_log_message := 'View '||v_obj_name_full||' was created successfully',
      p_location    := v_location);
   end if;
   -- grant permissions on object from template
   if p_grants_template is not null then 
  	perform ${target_schema}.f_grant_select(
  	  p_trg_table_name := v_obj_name_full, 
  	  p_src_table      := p_grants_template);
   end if;
     /*3.Collect statistic*/
   if p_analyze_flg  is true and p_mat_flg is true then
    perform ${target_schema}.f_analyze_table(p_table_name := v_obj_name_full); 
   end if;
  return v_cnt;
 exception when others then 
   raise notice 'ERROR loading table: %',SQLERRM;
      PERFORM ${target_schema}.f_write_log(        
      p_log_type    := 'ERROR',  
      p_log_message := 'ERROR while creating table '||v_obj_name,
      p_location    := v_location);
   return null;
 end;
$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION ${target_schema}.f_create_obj_sql(text, text, text, text, bool, text, bool, bool, text) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_create_obj_sql(text, text, text, text, bool, text, bool, bool, text) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_create_obj_sql(text, text, text, text, bool, text, bool, bool, text) TO "${owner}";
