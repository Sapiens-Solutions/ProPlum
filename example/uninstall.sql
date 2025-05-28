CREATE OR REPLACE FUNCTION f_uninstall_proplum_demo_example(p_demo_schema text, p_fw_schema text)
	RETURNS void 
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
    v_sql text;
    v_fw_schema text := p_fw_schema;--'etl';
    v_demo_schema text := p_demo_schema;--'proplum_demo';
    rec record;
BEGIN 
 --delete data, drop tables and schemas
   raise notice 'Start clear example';
   v_sql := 'select object_id, object_name from '||v_fw_schema||'.objects where load_group = ''DEMO_PROPLUM''';
   for rec in execute v_sql
     loop 
   	   v_sql = '
                delete from '||v_fw_schema||'.load_info where object_id = '||rec.object_id||';
                delete from '||v_fw_schema||'.objects   where object_id = '||rec.object_id||';
                delete from '||v_fw_schema||'.ext_tables_params where object_id = '||rec.object_id||';
                drop table if exists '||rec.object_name||';'
   	   ;
   	   raise notice 'Clear sql for object_id = %: [%]',rec.object_id, v_sql;
   	   execute v_sql;
     end loop;
    v_sql = 'drop schema if exists     '||v_demo_schema||' cascade;
             drop schema if exists stg_'||v_demo_schema||' cascade;';
    raise notice 'Drop demo schemas sql: [%]', v_sql;
    execute v_sql;

    v_sql = 'DROP FUNCTION if exists f_install_proplum_demo_example(text, text, text, _text, jsonb, jsonb);
             DROP FUNCTION if exists f_uninstall_proplum_demo_example(text, text);';
    raise notice 'Start drop demo functions sql: [%]', v_sql;
    execute v_sql;
END;
$$
EXECUTE ON ANY;