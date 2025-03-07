CREATE OR REPLACE FUNCTION ${target_schema}.f_load_scd2(
		p_load_id           int8, 
		p_table_from        text, 
		p_table_to          text default null,
        p_ignore_attributes _text default null, 
		p_primary_key       _text default null,
		p_start_date_column text default 'date_from',
		p_end_date_column   text default 'date_to', 
		p_curr_date         timestamp default '2999-01-01')
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$

/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2025*/
/*Function create history from non-historic table*/
/*
	 p_table_from - source table, contains actual state of attributes
	 p_table_to - target table with scd2
	 p_ignore_attributes - not time-dependent attributes in target table
	 p_primary_key - primary key in source and target tables
	 p_start_date_column - column in target table, which indicates start of activity period 
	 p_end_date_column - column in target table, which indicates end of activity period 
	 p_curr_date - max date in p_end_date_column of target table, which indicates actual state of source
*/
declare
	v_location text := '${target_schema}.f_load_scd2';
    v_object_id int8;
    v_table_from text;-- table with current state 
    v_table_to text;-- target table
    v_stg_schema text;
    v_sql text;
    v_columns_all _text;
    v_cnt int8 := 0;
    v_cnt_total int8;
    v_curr_date timestamp;
    v_max_date timestamp;
    v_primary_key _text;
    v_condition text;
    v_condition2 text;
    v_s1_table text;
    v_hist_table text;
    v_new_table text;
    v_act_table text;
    v_del_table text;
    v_all_table text;
    v_attributes _text;
    v_attributes_cond text;
    v_col_list text;
    v_where text;

begin
   perform ${target_schema}.f_write_log(
	 p_log_type := 'INFO', 
	 p_log_message := 'START ' || v_location || ' with load_id = ' || p_load_id, 
	 p_location := v_location,
	 p_load_id := p_load_id);

select o.object_id, o.object_name
 from ${target_schema}.objects o 
 join ${target_schema}.load_info li on o.object_id = li.object_id 
where li.load_id = p_load_id
 into v_object_id, v_table_to;
v_where = ${target_schema}.f_get_where_cond(p_load_id := p_load_id, p_table_alias:= 's');
v_table_from = ${target_schema}.f_unify_name(p_table_from);
v_table_to = coalesce(${target_schema}.f_unify_name(p_table_to),${target_schema}.f_unify_name(v_table_to));
v_curr_date  = coalesce(p_curr_date,'2999-01-01'::timestamp);
v_stg_schema = coalesce(${target_schema}.f_get_constant('c_stg_table_schema'),'stg_')||${target_schema}.f_get_table_schema(v_table_to);
v_primary_key = coalesce(p_primary_key, ${target_schema}.f_get_merge_key(v_object_id));
RAISE NOTICE 'v_primary_key: %', v_primary_key;

select 
 array_agg(column_name::text order by ordinal_position)  filter (where NOT  (column_name = any(v_primary_key)) and column_name not in (p_start_date_column, p_end_date_column) and NOT (column_name = any(p_ignore_attributes))), 
 array_agg(column_name::text ORDER BY ordinal_position)
  from information_schema.columns where table_schema||'.'||table_name = v_table_to 
  into v_attributes, 
       v_columns_all;
RAISE NOTICE 'v_attributes: %; v_columns_all: %', v_attributes, v_columns_all;


select string_agg('t.' || elem || ' = s.' || elem, ' and ')
 FROM unnest(v_primary_key) AS elem 
  into v_condition;

RAISE NOTICE 'v_condition: %', v_condition;

select regexp_replace(regexp_replace(string_agg('s.'|| elem||' '|| elem,', '),'(s\.|t\.)'||p_start_date_column,'current_date','g'),'(s\.|t\.)'||p_end_date_column, ''''||v_curr_date||'''::date','g')
 FROM unnest(v_columns_all) AS elem 
 into v_col_list;
RAISE NOTICE 'v_col_list: %', v_col_list;

select string_agg('t.' || elem || ' <> s.' || elem, ' or ') 
 FROM unnest(v_attributes) AS elem
 into v_attributes_cond;
RAISE NOTICE 'v_attributes_cond: %', v_attributes_cond;

select 'md5('||string_agg('coalesce(t.'||elem||'::text,''null''::text)','||')||')<>md5('||string_agg('coalesce(s.'||elem||'::text,''null''::text)','||')||')'
 FROM unnest(v_attributes) AS elem
 into v_attributes_cond;
RAISE NOTICE 'v_attributes_cond: %', v_attributes_cond;

--stage 1. find already existed and changed records

v_sql = 
  'select '||v_col_list||' from ' ||v_table_to||' t inner join '||v_table_from ||' s on '||v_condition||' where t.'||p_end_date_column||' = '''||v_curr_date||''' and ('|| v_attributes_cond ||' ) and ('||v_where||')';

perform ${target_schema}.f_write_log(
	p_log_type := 'SERVICE', 
    p_log_message := 'SQL to find changed records: '||coalesce(v_sql,'empty'), 
	p_location := v_location,
	p_load_id := p_load_id);

v_s1_table = 
  ${target_schema}.f_create_tmp_table(
   p_table_name := v_table_to, 
   p_prefix_name := 's1_',
   p_schema_name := v_stg_schema, 
   p_drop_table := true);

 v_cnt= ${target_schema}.f_insert_table_sql(
   p_sql := v_sql, 
   p_table_to := v_s1_table);
 
 if v_cnt is null then
   perform ${target_schema}.f_write_log(
     p_log_type := 'ERROR', 
     p_log_message := 'Insert data into temp table '|| v_s1_table ||' finished with error', 
     p_location := v_location);
   return false;
 end if;

 perform ${target_schema}.f_write_log(
	p_log_type := 'SERVICE', 
    p_log_message := 'End of stage 1: '||v_cnt||' records changed in '||v_table_from||'. Table '||v_s1_table||' was created',
	p_location := v_location,
	p_load_id := p_load_id);


--stage 2. find deleted records

select string_agg('t.' || elem || ' = s.' || elem, ' and ')
 FROM unnest(v_primary_key) AS elem 
  into v_condition;
RAISE NOTICE 'Stage 2: v_condition: %', coalesce(v_condition,'empty');

select 'select '||replace(string_agg('t.'|| elem ,','),'t.'||p_end_date_column, '(current_date-1) '||p_end_date_column)||' from '||v_table_to ||' t left join '||v_table_from ||' s on ('||v_condition||') where t.'||p_end_date_column||' = '''||v_curr_date||''' and ( s.'||v_primary_key[1]||' is null) and ('||v_where||')'
 FROM unnest(v_columns_all) AS elem
  into v_sql;
RAISE NOTICE 'Stage 2: v_sql: %', coalesce(v_sql,'empty');

perform ${target_schema}.f_write_log(
	p_log_type := 'SERVICE', 
    p_log_message := 'SQL to find deleted records: '||coalesce(v_sql,'empty'), 
	p_location := v_location,
	p_load_id := p_load_id);

v_del_table = 
  ${target_schema}.f_create_tmp_table(
   p_table_name := v_table_to, 
   p_prefix_name := 'del_',
   p_schema_name := v_stg_schema, 
   p_drop_table := true);

 v_cnt= ${target_schema}.f_insert_table_sql(
   p_sql := v_sql, 
   p_table_to := v_del_table);

 if v_cnt is null then
   perform ${target_schema}.f_write_log(
     p_log_type := 'ERROR', 
     p_log_message := 'Insert data into temp table '|| v_del_table ||' finished with error', 
     p_location := v_location);
   return false;
 end if;

 perform ${target_schema}.f_write_log(
	p_log_type := 'SERVICE', 
    p_log_message := 'End of stage 2: '||v_cnt||' records added in '||v_del_table,
	p_location := v_location,
	p_load_id := p_load_id);

--stage 3. create historic state for changed records

select string_agg('t.' || elem || ' = s1.' || elem, ' and ') 
 FROM unnest(v_primary_key) AS elem 
  into v_condition;
RAISE NOTICE 'Stage 3: v_condition: %', coalesce(v_condition,'empty');

select 'select '|| replace(string_agg('t.'|| elem ,','),'t.'||p_end_date_column,'(current_date-1) '||p_end_date_column)|| ' from '||v_table_to||' t inner join '||v_s1_table||' s1 on ('||v_condition||') where (t.'||p_end_date_column||' = '''||v_curr_date||''')
   and (t.'||p_start_date_column||'<> current_date ) --same day change, rewrite actual state without adding history
  union all
   select '|| string_agg(elem ,',')||' from '||v_del_table
 FROM unnest(v_columns_all) AS elem
  into v_sql;
RAISE NOTICE 'Stage 3: v_sql: %', coalesce(v_sql,'empty');

perform ${target_schema}.f_write_log(
	p_log_type := 'SERVICE', 
    p_log_message := 'SQL to build history for changed and deleted records: '||coalesce(v_sql,'empty'), 
	p_location := v_location,
	p_load_id := p_load_id);

v_hist_table = 
  ${target_schema}.f_create_tmp_table(
   p_table_name := v_table_to, 
   p_prefix_name := 'hist_',
   p_schema_name := v_stg_schema, 
   p_drop_table := true);

 v_cnt= ${target_schema}.f_insert_table_sql(
   p_sql := v_sql, 
   p_table_to := v_hist_table);

 if v_cnt is null then
   perform ${target_schema}.f_write_log(
     p_log_type := 'ERROR', 
     p_log_message := 'Insert data into temp table '|| v_hist_table ||' finished with error', 
     p_location := v_location);
   return false;
 end if;

 perform ${target_schema}.f_write_log(
	p_log_type := 'SERVICE', 
    p_log_message := 'End of stage 3: '||v_cnt||' records added in '||v_hist_table,
	p_location := v_location,
	p_load_id := p_load_id);
v_cnt_total = v_cnt;

--stage 4. find new records

select string_agg('t.' || elem || ' = s.' || elem, ' and ')
 FROM unnest(v_primary_key) AS elem 
  into v_condition;
RAISE NOTICE 'Stage 4: v_condition: %', coalesce(v_condition,'empty');

select 'select '||replace(replace(string_agg('s.'|| elem ,','),'s.'||p_start_date_column,'current_date'),'s.'||p_end_date_column, ''''||v_curr_date||''' '||p_end_date_column)||' from '||v_table_from||' s left join '||v_table_to||' t on ('||v_condition||') and t.'||p_end_date_column||' = '''||v_curr_date||''' where ( t.'||v_primary_key[1]||' is null) and ('||v_where||')'
 FROM unnest(v_columns_all) AS elem
  into v_sql;
RAISE NOTICE 'Stage 4: v_sql: %', coalesce(v_sql,'empty');

perform ${target_schema}.f_write_log(
	p_log_type := 'SERVICE', 
    p_log_message := 'SQL to find new records: '||coalesce(v_sql,'empty'), 
	p_location := v_location,
	p_load_id := p_load_id);

v_new_table = 
  ${target_schema}.f_create_tmp_table(
   p_table_name := v_table_to, 
   p_prefix_name := 'new_',
   p_schema_name := v_stg_schema, 
   p_drop_table := true);

 v_cnt= ${target_schema}.f_insert_table_sql(
   p_sql := v_sql, 
   p_table_to := v_new_table);

 if v_cnt is null then
   perform ${target_schema}.f_write_log(
     p_log_type := 'ERROR', 
     p_log_message := 'Insert data into temp table '|| v_new_table ||' finished with error', 
     p_location := v_location);
   return false;
 end if;

 perform ${target_schema}.f_write_log(
	p_log_type := 'SERVICE', 
    p_log_message := 'End of stage 4: '||v_cnt||' new records were found in '||v_table_from,
	p_location := v_location,
	p_load_id := p_load_id);


--stage 5. create new actual state of table 

select string_agg('s1.' || elem || ' = t.' || elem, ' and '), string_agg('t.' || elem || ' = d.' || elem, ' and ')
 FROM unnest(v_primary_key) AS elem 
  into v_condition, v_condition2;
RAISE NOTICE 'Stage 5: v_condition: %', coalesce(v_condition,'empty');

select 
'select * from '||v_s1_table||' s1 --changed
  union all 
 select '||string_agg('t.'|| elem ,', ')||' 
  from '||v_table_to ||' t 
    left join '||v_s1_table||' s1 on ('||v_condition||') 
   where (s1.'||v_primary_key[1]||' is null) and (t.'||p_end_date_column||' = '''||v_curr_date||''')
     and not exists (select 1 from '||v_del_table ||' d where ('||v_condition2||') and (t.'||p_end_date_column||' = '''||v_curr_date||''')) --not deleted and not changed
  union all 
 select * from '||v_new_table
from unnest(v_columns_all) AS elem
into v_sql;
RAISE NOTICE 'Stage 5: v_sql: %', coalesce(v_sql,'empty');

v_act_table = 
  ${target_schema}.f_create_tmp_table(
   p_table_name := v_table_to, 
   p_prefix_name := 'act_',
   p_schema_name := v_stg_schema, 
   p_drop_table := true);

 v_cnt= ${target_schema}.f_insert_table_sql(
   p_sql := v_sql, 
   p_table_to := v_act_table);

 if v_cnt is null then
   perform ${target_schema}.f_write_log(
     p_log_type := 'ERROR', 
     p_log_message := 'Insert data into table '|| v_act_table ||' finished with error', 
     p_location := v_location);
   return false;
 end if;
 perform ${target_schema}.f_write_log(
	p_log_type := 'SERVICE', 
    p_log_message := 'End of stage 5: '||v_cnt||' records added in actual state table '||v_act_table,
	p_location := v_location,
	p_load_id := p_load_id);

 v_cnt_total = v_cnt_total + v_cnt;

--check table has partitions
select count(1)
  into v_cnt
  from pg_partitions p
  where p.schemaname||'.'||p.tablename = v_table_to;

if v_cnt >= 2 then
--stage 6. exchange partition with actuall state
 perform 
   ${target_schema}.f_switch_partition(
     p_table_name := v_table_to, 
     p_partition_value := v_curr_date, 
     p_switch_table_name := v_act_table);

--stage 7. update historical records
 v_primary_key = array_append(v_primary_key, p_end_date_column);

 v_sql = 'select max('||p_end_date_column||') from '||v_hist_table;
 RAISE NOTICE 'Stage 7: v_sql: %', coalesce(v_sql,'empty');
 execute v_sql into v_max_date;

 RAISE NOTICE 'Stage 7: v_max_date: %', coalesce(v_max_date::text,'empty');
-- create partitions for new data 
 if v_max_date is not null then  
   perform ${target_schema}.f_create_date_partitions(
      p_table_name      := v_table_to, 
      p_partition_value := v_max_date,
      p_limit_value     := v_curr_date - interval'1 day');
   v_cnt = 
    ${target_schema}.f_load_partitions(
      p_load_id := p_load_id,
      p_table_from_name := v_hist_table,
      p_table_to_name := v_table_to,
      p_merge_partitions := true,
      p_merge_key := v_primary_key
    );

    if v_cnt is null then
     perform ${target_schema}.f_write_log(
        p_log_type := 'ERROR', 
        p_log_message := 'Insert data into table ' || v_table_to || ' from ' || v_hist_table || ' finished with error', 
        p_location := v_location);
     return false;
    end if;
 end if;

 else --table has no partitions
   v_primary_key = array_append(v_primary_key, p_end_date_column);
   v_sql = 
    'select * from '||v_act_table||'
      union all
     select * from '||v_hist_table;
 
   RAISE NOTICE 'Stage 6: v_sql: %', coalesce(v_sql,'empty');

   --prepare table with all records to be changed
   v_all_table = 
     ${target_schema}.f_create_tmp_table(
      p_table_name := v_table_to, 
      p_prefix_name := 'all_',
      p_schema_name := v_stg_schema, 
      p_drop_table := true);

   v_cnt= ${target_schema}.f_insert_table_sql(
      p_sql := v_sql, 
      p_table_to := v_all_table);

 if v_cnt is null then
   perform ${target_schema}.f_write_log(
     p_log_type := 'ERROR', 
     p_log_message := 'Insert data into table '|| v_all_table ||' finished with error', 
     p_location := v_location);
   return false;
 end if;
 perform ${target_schema}.f_write_log(
	p_log_type := 'SERVICE', 
    p_log_message := 'End of stage 6: '||v_cnt||' records added in table '||v_all_table,
	p_location := v_location,
	p_load_id := p_load_id);
   --delete current state in target table
   v_sql = 'DELETE FROM '||v_table_to||' where '||p_end_date_column||'='''||v_curr_date||'''';
   execute v_sql;

   v_cnt_total = 
     ${target_schema}.f_upsert_table(
       p_load_id := p_load_id,
       p_table_from_name := v_all_table, 
       p_table_to_name := v_table_to, 
       p_delete_duplicates := true,
       p_merge_key := v_primary_key);
    execute 'drop table '||v_all_table;
    if v_cnt_total is null then
     perform ${target_schema}.f_write_log(
        p_log_type := 'ERROR', 
        p_log_message := 'Upsert data into table ' || v_table_to || ' from ' || v_all_table || ' finished with error', 
        p_location := v_location);
     return false;
    end if;
end if;


--drop temporary tables
v_sql = 'drop table '||v_s1_table ||';'||
 'drop table '||v_hist_table ||';'||
 'drop table '||v_act_table ||';'||
 'drop table '||v_del_table ||';';

execute v_sql;

perform ${target_schema}.f_write_log(
   p_log_type := 'SERVICE', 
   p_log_message := 'Start drop temporary tables with sql: '||coalesce(v_sql,'empty'),
   p_location := v_location,
   p_load_id := p_load_id);

perform ${target_schema}.f_set_load_id_success(p_load_id);
perform ${target_schema}.f_update_load_info(
   p_load_id := p_load_id, 
   p_field_name := 'row_cnt', 
   p_value := v_cnt_total::text);
perform ${target_schema}.f_write_log(
   p_log_type := 'SERVICE', 
   p_log_message := 'END '|| v_location || ' with load_id = ' || p_load_id,
   p_location := v_location,
   p_load_id := p_load_id);

return true;

exception when others then
 perform ${target_schema}.f_write_log(
	p_log_type := 'ERROR', 
    p_log_message := 'Run ' || v_location || ' finished with error: ' || sqlerrm, 
	p_location := v_location,
	p_load_id := p_load_id);
 return false;
end


$$
EXECUTE ON ANY;
-- Permissions
ALTER FUNCTION ${target_schema}.f_load_scd2(int8, text, text, _text, _text, text, text, timestamp) OWNER TO "${owner}";
GRANT ALL ON FUNCTION ${target_schema}.f_load_scd2(int8, text, text, _text, _text, text, text, timestamp) TO public;
GRANT ALL ON FUNCTION ${target_schema}.f_load_scd2(int8, text, text, _text, _text, text, text, timestamp) TO "${owner}";