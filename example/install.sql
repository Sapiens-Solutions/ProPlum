CREATE OR REPLACE FUNCTION f_install_proplum_demo_example(p_demo_schema text, p_owner text, p_fw_schema text, p_demo _text, p_gp_conn jsonb default null, p_gpfdist_params jsonb default null)
	RETURNS void 
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
    i int := 1;
    v_sql text;
    demo_schema text := p_demo_schema;--'proplum_demo';
    v_owner text := p_owner;--'fw_user';
    v_object int8;
    v_object_gpfdist int8;
    v_fw_schema text := p_fw_schema;--'etl';
    v_demo _text := p_demo;--'{pxf,function,gpfdist}';
    v_gp_conn jsonb := p_gp_conn;--'{"host":"greenplum_host","port":"greenplum_port","db":"greenplum_db","user":"greenplum_user","pass":"greenplum_user_password"}';
    v_gpfdist_params jsonb:= p_gpfdist_params;--'{"host":"gpfdist_host","port":"gpfdist_port"}';
BEGIN    
--create schemas
    raise notice 'Stage %. Start create schemas: %, stg_%',i,demo_schema,demo_schema;
    v_sql := 'create schema if not exists '||demo_schema||' AUTHORIZATION '||v_owner||';
                 create schema if not exists stg_'||demo_schema||' AUTHORIZATION '||v_owner||';';
    raise notice 'Stage %. Create schemas with sql: [%]',i,v_sql;
    execute v_sql;
if 'pxf' = any(v_demo) and v_gp_conn is not null then
--example 1. pxf
    i = i + 1;
--create tables
--source table
    raise notice 'Stage %. Start create tables: %.demo_pxf_source, %.demo_pxf_target',i,demo_schema,demo_schema;
    v_sql := '
    DROP TABLE if exists '||demo_schema||'.demo_pxf_source;
	create table '||demo_schema||'.demo_pxf_source
	(
	    int_column INT,
	    text_column TEXT,
	    char_column CHAR(10),
	    decimal_column DECIMAL(10, 2),
	    json_column JSON,
	    array_text_column TEXT[],
	    date_column DATE,
	    timestamp_column TIMESTAMP
	)
	WITH (
		appendonly=true,
		orientation=column,
		compresstype=zstd,
		compresslevel=1
	)
	DISTRIBUTED by(int_column);
	ALTER TABLE '||demo_schema||'.demo_pxf_source OWNER TO '||v_owner||';
	GRANT ALL ON TABLE '||demo_schema||'.demo_pxf_source TO '||v_owner||';
    ';
    raise notice 'Stage %. Create table demo_pxf_source with sql: [%]',i,v_sql;
    EXECUTE v_sql;

--target table
    v_sql := '
    DROP TABLE if exists '||demo_schema||'.demo_pxf_target;
    CREATE TABLE '||demo_schema||'.demo_pxf_target (
        int_column INT,
        text_column TEXT,
        char_column CHAR(10),
        decimal_column DECIMAL(10, 2),
        json_column JSON,
        array_text_column TEXT[],
        date_column DATE,
        timestamp_column TIMESTAMP
    )
	WITH (
		appendonly=true,
		orientation=column,
		compresstype=zstd,
		compresslevel=1
	)
    DISTRIBUTED BY (int_column)
    PARTITION BY RANGE (date_column) (
        START (''' || date_trunc('month', current_date - INTERVAL '3 months') || ''') END (''' || date_trunc('month', current_date - INTERVAL '1 months') || ''') EVERY (INTERVAL ''1 month''),
        START (''' || date_trunc('month', current_date - INTERVAL '1 month') || ''') END (''' || current_date - INTERVAL '1 week' || ''') EVERY (INTERVAL ''1 day''),
        default partition p_default
    );
	ALTER TABLE '||demo_schema||'.demo_pxf_target OWNER TO '||v_owner||';
	GRANT ALL ON TABLE '||demo_schema||'.demo_pxf_target TO '||v_owner||';
';
    raise notice 'Stage %. Create table demo_pxf_target with sql: [%]',i,v_sql;
    EXECUTE v_sql;

i = i + 1;
--insert data into source
raise notice 'Stage %. Start generate data into table demo_pxf_source',i;
	v_sql := '
	INSERT INTO '||demo_schema||'.demo_pxf_source (
	    int_column,
	    text_column,
	    char_column,
	    decimal_column,
	    json_column,
	    array_text_column,
	    date_column,
	    timestamp_column
	)
	SELECT 
	    i AS int_column,
	    ''Text '' || (random() * 1000)::INT AS text_column,
	    substr(md5(random()::TEXT), 1, 10) AS char_column,
	    (random() * 1000)::DECIMAL(10, 2) AS decimal_column,
	    (''{"id": '' || i || '', "value": "'' || md5(random()::TEXT) || ''"}'')::JSON AS json_column,
	    ARRAY[''item1'', ''item'' || (random() * 10)::INT, ''item'' || (random() * 100)::INT] AS array_text_column,
	    (current_date - (random() * 90)::INT) AS date_column,
	    ((current_date - (random() * 90)::INT) + 
	     (INTERVAL ''1 minute'' + (random() * 1440)::INT * INTERVAL ''1 minute'')) AS timestamp_column
	FROM 
	    generate_series(1, 10000) AS i;
	';
    raise notice 'Stage %. Insert data into table demo_pxf_source with sql: [%]',i,v_sql;
	execute v_sql;
    i = i + 1;

-- insert into objects
--find free object_id without history
raise notice 'Stage %. Start add settings into objects',i;
execute '
  select free_obj
	FROM generate_series(1,10000) free_obj
      LEFT OUTER join '||v_fw_schema||'.objects obj ON free_obj = obj.object_id
    where obj.object_id IS null
      and not exists (select 1 from '||v_fw_schema||'.load_info li where obj.object_id = li.object_id)
     order by free_obj asc 
    limit 1;'  into v_object;
--generate objects entry

v_sql := '
INSERT INTO '||v_fw_schema||'.objects
(object_id, object_name, object_desc, extraction_type, load_type, merge_key, delta_field, delta_field_format, delta_safety_period, bdate_field, bdate_field_format, bdate_safety_period, load_method, job_name, responsible_mail, priority, periodicity, load_interval, activitystart, activityend, "active", load_start_date, delta_start_date, delta_mode, connect_string, load_function_name, where_clause, load_group, src_date_type, src_ts_type, column_name_mapping, transform_mapping, delta_field_type, bdate_field_type)
VALUES( '||v_object||', '''||demo_schema||'.demo_pxf_target'', ''Proplum demo: load from pxf'', ''DELTA'', ''UPDATE_PARTITION'', ''{int_column}'', ''timestamp_column'', NULL, ''00:00:00''::interval, ''date_column'', ''YYYY-MM-DD'', ''00:00:00''::interval, ''pxf'', ''job_proplum_demo_dag_no_dep'',NULL, 1, ''1 day''::interval, NULL, ''00:00:00'', ''23:59:59'', true, ''2021-01-01 00:00:00.000'', ''2000-01-01 00:00:00.000'', ''DELTA'', '''||demo_schema||'.demo_pxf_source?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://'||(v_gp_conn->>'host')||':'||(v_gp_conn->>'port')||'/'||(v_gp_conn->>'db')||'&USER='||(v_gp_conn->>'user')||'&PASS='||(v_gp_conn->>'pass')||''', NULL, NULL, ''DEMO_PROPLUM'', NULL, NULL, NULL::jsonb, ''{"json_column": "json_column::json", "array_text_column": "array_text_column::_text"}''::jsonb, NULL, NULL);
';
raise notice 'Stage %: sql for objects: [%]',i,v_sql;
execute v_sql;
end if;

if 'function' = any(v_demo)  then
i = i + 1;
--example 2. function
--create tables
--source table
    raise notice 'Stage %. Start create tables: %.demo_func_source, %.demo_func_target',i,demo_schema,demo_schema;
    v_sql := '
    DROP TABLE if exists '||demo_schema||'.demo_func_source;
	create table '||demo_schema||'.demo_func_source
	(
	    int_column INT,
	    text_column TEXT,
	    char_column CHAR(10),
	    decimal_column DECIMAL(10, 2),
	    json_column JSON,
	    array_text_column TEXT[],
	    date_column DATE,
	    timestamp_column TIMESTAMP
	)
	WITH (
		appendonly=true,
		orientation=column,
		compresstype=zstd,
		compresslevel=1
	)
	DISTRIBUTED by(int_column);
	ALTER TABLE '||demo_schema||'.demo_func_source OWNER TO '||v_owner||';
	GRANT ALL ON TABLE '||demo_schema||'.demo_func_source TO '||v_owner||';
    ';
    raise notice 'Stage %. Create table demo_func_source with sql: [%]',i,v_sql;
    EXECUTE v_sql;

--target table
    v_sql := '
    DROP TABLE if exists '||demo_schema||'.demo_func_target;
    CREATE TABLE '||demo_schema||'.demo_func_target (
        int_column INT,
        decimal_column_sum DECIMAL(10, 2),
        date_column DATE,
        timestamp_column TIMESTAMP
    )
	WITH (
		appendonly=true,
		orientation=column,
		compresstype=zstd,
		compresslevel=1
	)
    DISTRIBUTED BY (int_column)
    PARTITION BY RANGE (date_column) (
        START (''' || date_trunc('month', current_date - INTERVAL '3 months') || ''') END (''' || date_trunc('month', current_date - INTERVAL '1 months') || ''') EVERY (INTERVAL ''1 month''),
        START (''' || date_trunc('month', current_date - INTERVAL '1 month') || ''') END (''' || current_date - INTERVAL '1 week' || ''') EVERY (INTERVAL ''1 day''),
        default partition p_default
    );
	ALTER TABLE '||demo_schema||'.demo_func_target OWNER TO '||v_owner||';
	GRANT ALL ON TABLE '||demo_schema||'.demo_func_target TO '||v_owner||';
';
    raise notice 'Stage %. Create table demo_func_target with sql: [%]',i,v_sql;
    EXECUTE v_sql;

i = i + 1;
--insert data into source
raise notice 'Stage %. Start generate data into table demo_func_source',i;
	v_sql := '
	INSERT INTO '||demo_schema||'.demo_func_source (
	    int_column,
	    text_column,
	    char_column,
	    decimal_column,
	    json_column,
	    array_text_column,
	    date_column,
	    timestamp_column
	)
	SELECT 
	    (random()*1000)::int AS int_column,
	    ''Text '' || (random() * 1000)::INT AS text_column,
	    substr(md5(random()::TEXT), 1, 10) AS char_column,
	    (random() * 1000)::DECIMAL(10, 2) AS decimal_column,
	    (''{"id": '' || i || '', "value": "'' || md5(random()::TEXT) || ''"}'')::JSON AS json_column,
	    ARRAY[''item1'', ''item'' || (random() * 10)::INT, ''item'' || (random() * 100)::INT] AS array_text_column,
	    (current_date - (random() * 90)::INT) AS date_column,
	    ((current_date - (random() * 90)::INT) + 
	     (INTERVAL ''1 minute'' + (random() * 1440)::INT * INTERVAL ''1 minute'')) AS timestamp_column
	FROM 
	    generate_series(1, 10000) AS i;
	';
    raise notice 'Stage %. Insert data into table demo_func_source with sql: [%]',i,v_sql;
	execute v_sql;
    i = i + 1;

-- insert into objects
--find free object_id without history
raise notice 'Stage %. Start add settings into objects',i;
execute '
  select free_obj
	FROM generate_series(1,10000) free_obj
      LEFT OUTER join '||v_fw_schema||'.objects obj ON free_obj = obj.object_id
    where obj.object_id IS null
      and not exists (select 1 from '||v_fw_schema||'.load_info li where obj.object_id = li.object_id)
     order by free_obj asc 
    limit 1;'  into v_object;
--generate objects entry

v_sql := '
INSERT INTO '||v_fw_schema||'.objects
(object_id, object_name, object_desc, extraction_type, load_type, merge_key, delta_field, delta_field_format, delta_safety_period, bdate_field, bdate_field_format, bdate_safety_period, load_method, job_name, responsible_mail, priority, periodicity, load_interval, activitystart, activityend, "active", load_start_date, delta_start_date, delta_mode, connect_string, load_function_name, where_clause, load_group, src_date_type, src_ts_type, column_name_mapping, transform_mapping, delta_field_type, bdate_field_type)
VALUES( '||v_object||', '''||demo_schema||'.demo_func_target'', ''Proplum demo: load from function'', ''DELTA'', ''UPDATE_PARTITION'', ''{int_column}'', ''timestamp_column'', NULL, ''00:00:00''::interval, ''date_column'', ''YYYY-MM-DD'', ''00:00:00''::interval, ''function'', ''job_proplum_demo_dag_no_dep'',NULL, 1, ''1 day''::interval, NULL, ''00:00:00'', ''23:59:59'', true, ''2021-01-01 00:00:00.000'', ''2000-01-01 00:00:00.000'', ''DELTA'', NULL, '''||v_fw_schema||'.f_load_simple($load_id,''''proplum_demo.demo_func_source'''')'', NULL, ''DEMO_PROPLUM'', NULL, NULL, NULL::jsonb, ''{"decimal_column_sum": "sum(decimal_column)","timestamp_column":"current_timestamp", "additional": "group by int_column, date_column"}''::jsonb, NULL, NULL);
';
raise notice 'Stage %: sql for objects: [%]',i,v_sql;
execute v_sql;
end if;

if 'gpfdist' = any(v_demo) and v_gpfdist_params is not null then
i = i + 1;
--example 3. gpfdist
--create tables

--target table
    v_sql := '
    DROP TABLE if exists '||demo_schema||'.demo_gpfdist_target;
    CREATE TABLE '||demo_schema||'.demo_gpfdist_target (
	    int_column INT,
	    text_column TEXT,
	    char_column CHAR(10),
	    decimal_column DECIMAL(10, 2),
	    json_column JSON,
	    array_text_column TEXT[],
	    date_column DATE,
	    timestamp_column TIMESTAMP
    )
	WITH (
		appendonly=true,
		orientation=column,
		compresstype=zstd,
		compresslevel=1
	)
    DISTRIBUTED BY (int_column)
    PARTITION BY RANGE (date_column) (
        START (''' || date_trunc('month', current_date - INTERVAL '3 months') || ''') END (''' || date_trunc('month', current_date - INTERVAL '1 months') || ''') EVERY (INTERVAL ''1 month''),
        START (''' || date_trunc('month', current_date - INTERVAL '1 month') || ''') END (''' || current_date - INTERVAL '1 week' || ''') EVERY (INTERVAL ''1 day''),
        default partition p_default
    );
	ALTER TABLE '||demo_schema||'.demo_gpfdist_target OWNER TO '||v_owner||';
	GRANT ALL ON TABLE '||demo_schema||'.demo_gpfdist_target TO '||v_owner||';
';
    raise notice 'Stage %. Create table demo_gpfdist_target with sql: [%]',i,v_sql;
    EXECUTE v_sql;

i = i + 1;

-- insert into objects
--find free object_id without history
raise notice 'Stage %. Start add settings into objects',i;
execute '
  select free_obj
	FROM generate_series(1,10000) free_obj
      LEFT OUTER join '||v_fw_schema||'.objects obj ON free_obj = obj.object_id
    where obj.object_id IS null
      and not exists (select 1 from '||v_fw_schema||'.load_info li where obj.object_id = li.object_id)
     order by free_obj asc 
    limit 1;'  into v_object_gpfdist;
--generate objects entry

v_sql := '
INSERT INTO '||v_fw_schema||'.objects
(object_id, object_name, object_desc, extraction_type, load_type, merge_key, delta_field, delta_field_format, delta_safety_period, bdate_field, bdate_field_format, bdate_safety_period, load_method, job_name, responsible_mail, priority, periodicity, load_interval, activitystart, activityend, "active", load_start_date, delta_start_date, delta_mode, connect_string, load_function_name, where_clause, load_group, src_date_type, src_ts_type, column_name_mapping, transform_mapping, delta_field_type, bdate_field_type)
VALUES( '||v_object_gpfdist||', '''||demo_schema||'.demo_gpfdist_target'', ''Proplum demo: load from gpfdist'', ''FULL'', ''UPDATE_PARTITION'', ''{int_column}'', ''timestamp_column'', NULL, ''00:00:00''::interval, ''date_column'', ''YYYY-MM-DD'', ''00:00:00''::interval, ''gpfdist'', ''job_proplum_demo_dag_no_dep'',NULL, 1, ''1 day''::interval, NULL, ''00:00:00'', ''23:59:59'', true, ''2021-01-01 00:00:00.000'', ''2000-01-01 00:00:00.000'', ''DELTA'', NULL, NULL, NULL, ''DEMO_PROPLUM'', NULL, NULL, NULL::jsonb, ''{"json_column":"json_column::json", "array_text_column": "array_text_column::_text"}''::jsonb, NULL, NULL);
INSERT INTO '||v_fw_schema||'.ext_tables_params
(object_id, load_method, connection_string, additional, "active", object_name)
VALUES('||v_object_gpfdist||', ''gpfdist'', ''location (''''gpfdist://'||(v_gpfdist_params->>'host')||':'||(v_gpfdist_params->>'port')||'/gpfdist_data.csv'''') on all FORMAT ''''TEXT'''' (delimiter '''';'''' null '''''''')ENCODING ''''UTF8'''' SEGMENT REJECT LIMIT 10 ROWS;'', NULL, true, NULL);
';
raise notice 'Stage %: sql for objects: [%]',i,v_sql;
execute v_sql;
end if;

if ARRAY['pxf','function','gpfdist']  <@ v_demo then
 i = i + 1;
 raise notice 'Stage %. Start add dependencies',i;
 v_sql := '
 delete from '||v_fw_schema||'.dependencies where object_id = '||v_object_gpfdist||';
 INSERT INTO '||v_fw_schema||'.dependencies
  (object_id, object_id_depend)
   VALUES('||v_object_gpfdist||','|| v_object||');';
 raise notice 'Stage %: sql for dependencies: [%]',i,v_sql;
 execute v_sql;
end if;
 END;
$$
EXECUTE ON ANY;