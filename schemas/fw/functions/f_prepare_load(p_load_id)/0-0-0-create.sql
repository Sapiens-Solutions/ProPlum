CREATE OR REPLACE FUNCTION fw.f_prepare_load(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
	/*preparing operations before loading*/
	DECLARE
	  v_location        text := 'fw.f_prepare_load';
	  v_load_type       text;
	  v_extraction_type text;
	  v_table_name      text;
	  v_full_table_name text;
	  v_load_method     text;
	  v_tmp_schema_name text;
	  v_error           text;
	  v_schema_name     text;
	  v_tmp_prefix      text;
	  v_tmp_suffix      text;
	  v_ext_prefix      text;
	  v_ext_suffix      text;
	  v_res             bool;
 	BEGIN
    --Log
    perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START Before load processing for table '||v_table_name, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
     
	--set load_id for session
    --perform set_config('fw.load_id', p_load_id::text, false);
    perform fw.f_set_session_param(
       p_param_name  := 'fw.load_id',
       p_param_value := p_load_id::text);
   
    -- Get table load type
    select ob.object_name, coalesce(li.extraction_type, ob.extraction_type), coalesce(li.load_type, ob.load_type), coalesce(li.load_method, ob.load_method)
    into   v_full_table_name, v_extraction_type ,v_load_type, v_load_method
    from   fw.load_info li
    join   fw.objects ob on(ob.object_Id = li.object_Id)
    where  li.load_id = p_load_id;
    v_full_table_name  = fw.f_unify_name(p_name := v_full_table_name); -- full table name
    v_schema_name = fw.f_get_table_schema(v_full_table_name);--left(v_full_table_name,position('.' in v_full_table_name)-1); -- target table schema name
    v_table_name =  right(v_full_table_name,length(v_full_table_name) - POSITION('.' in v_full_table_name));-- table name wo schema
    v_schema_name = replace(replace(v_schema_name,'src_',''),'stg_','');
    v_tmp_schema_name = coalesce(
      fw.f_get_constant(
      p_constant_name := 'c_stg_table_schema'),
      'stg_')||v_schema_name;
    v_tmp_prefix = coalesce(
      fw.f_get_constant(
      p_constant_name := 'c_delta_table_prefix'),
      'delta_');
    v_tmp_suffix = '_'||p_load_id::text; 
    v_ext_prefix = coalesce(
      fw.f_get_constant(
      p_constant_name := 'c_ext_table_prefix'),
      'ext_');
    v_ext_suffix = '_'||p_load_id::text;
    perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'Check variables:  v_tmp_schema_name: '||coalesce(v_tmp_schema_name,'{empty}'), 
       p_location    := v_location,
       p_load_id     := p_load_id); --log function call
    perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'Check variables:  v_tmp_prefix: '||coalesce(v_tmp_prefix,'{empty}'), 
       p_location    := v_location,
       p_load_id     := p_load_id); --log function call
    perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'Check variables:  v_ext_prefix: '||coalesce(v_ext_prefix,'{empty}'), 
       p_location    := v_location,
       p_load_id     := p_load_id); --log function call
       
    IF v_load_type in (select distinct load_type from fw.d_load_type) and v_extraction_type in (select distinct extraction_type from fw.d_extraction_type) THEN
        --Creates work tables
    raise notice '1. Start creating external table with params: p_table_name: {%}, p_load_method: {%}, p_schema_name: {%}, p_prefix: {%}',
                  coalesce(v_full_table_name,'{empty}'),coalesce(v_load_method,'{empty}'),coalesce(v_tmp_schema_name,'{empty}'),coalesce(v_ext_prefix,'{empty}');
        PERFORM fw.f_create_ext_table(
           p_table_name  := v_full_table_name, 
           p_load_method := v_load_method, 
           p_schema_name := v_tmp_schema_name,
           p_prefix := v_ext_prefix,
           p_suffix := v_ext_suffix,
           p_load_id := p_load_id);
    raise notice '2. Start creating delta table with params: p_table_name: {%}, p_schema_name: {%}, p_prefix_name: {%}',
                  coalesce(v_full_table_name,'{empty}'),coalesce(v_tmp_schema_name,'{empty}'),coalesce(v_tmp_prefix,'{empty}');
		PERFORM fw.f_create_tmp_table(
	       p_table_name  := v_full_table_name,
	       p_schema_name := v_tmp_schema_name,
	       p_prefix_name := v_tmp_prefix,
	       p_suffix_name := v_tmp_suffix,
	       p_drop_table  := true);
	    v_res = true;
    ELSE
        v_error := 'Unable to process extraction ('||coalesce(v_extraction_type,'empty')||') or load ('||coalesce(v_load_type,'empty')||') types ';
        perform fw.f_write_log(
           p_log_type    := 'ERROR', 
           p_log_message := 'Error while processing before job tasks: '||v_error, 
           p_location    := v_location,
           p_load_id     := p_load_id); --log function call
        return false;
    END IF;

  -- Log 
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'END Before job tasks processing for table '||v_full_table_name,
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  return v_res;
 exception when others then 
     raise notice 'ERROR while prepare loading table %: %',v_table_name,SQLERRM;
     PERFORM fw.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Prepare loading into table '||v_full_table_name||' finished with error: '||SQLERRM, 
        p_location    := v_location,
        p_load_id     := p_load_id);
     perform fw.f_set_load_id_error(p_load_id := p_load_id);  
     return false;
END;


$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION fw.f_prepare_load(int8) OWNER TO "admin";
GRANT ALL ON FUNCTION fw.f_prepare_load(int8) TO public;
GRANT ALL ON FUNCTION fw.f_prepare_load(int8) TO "admin";
