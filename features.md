# Особенности технической реализации

## Логирование загрузок

Из-за особенностей работы транзакций в Greenplum, просмотр данных логирования в таблице logs доступен только в случае успешного завершения транзакции, т.е. когда не произошел rollback. В связи с этим реализация записи логов сделана через открытие автономной транзакции postgres через dblink.

Последовательность реализации:

1. Включение/создание расширения postgres_fdw (foreign data wrapper):
   ```
   CREATE EXTENSION postgres_fdw;
   ```
2. Создание сервера postgres_fdw со ссылкой на БД (postgres или greenplum)
   ```
   CREATE SERVER <имя сервера>
   FOREIGN DATA WRAPPER postgres_fdw
   OPTIONS (host '<ip БД/dns-имя>', port '5432', dbname '<имя базы данных>');
   ```
3. Выдача полномочий пользователям, под которым будет настроен вызов функций
   ```
   GRANT USAGE ON FOREIGN SERVER <имя сервера> TO <имя пользователя>;
   ```
4. Создание мэппинга пользователя текущей БД на пользователя удаленной БД.
   ```
   CREATE USER MAPPING FOR <имя пользователя>
   SERVER <имя сервера>
   OPTIONS (user '<имя пользователя удаленной бд>', password '<пароль пользователя удаленной бд>');
   ```
5. Включение/создание расширения dblink
   ```
   CREATE EXTENSION dblink;
   ```
6. Вызов в функции оператора dblink в связке с postgres_fdw-сервером. Например, в функции f_write_log:
   ```
   SECURITY DEFINER
   AS $$   
   DECLARE
   ...
     v_sql text;
     v_res    text;
     v_server text := 'adwh_server';
   ...
   BEGIN
   ...
     v_sql := 'INSERT INTO logs(log_id, load_id, log_type, log_msg, log_location, is_error,log_timestamp,log_user)
               VALUES ( '||nextval('etl.log_id_seq')||' , '||coalesce(v_load_id, 0)||', '''||v_log_type||''', '||coalesce(''''||v_log_message||'''','''empty''')||', '||coalesce(''''||v_location||'''','null')||', '||case when v_log_type='ERROR' THEN true else false end||',current_timestamp,current_user);';
      --dblink for fdw server
     v_res := dblink(v_server,v_sql);
   ...
   ALTER FUNCTION f_write_log(text,text,text,int8) OWNER TO <имя пользователя>;
   ```


## Промежуточные таблицы, формирующиеся при работе

В процессе загрузки данных из исходной системы в схеме для стейджинга, указанной в таблице load_constants, для каждой целевой таблицы автоматически создаются и заполняются следующие промежуточные таблицы:

| Таблица                                        | Описание                                                 | FULL | DELTA | DELTA_<br/>MERGE | DELTA_<br/>PARTITION | DELTA_<br/>UPSERT |   
|------------------------------------------------|----------------------------------------------------------|------|-------|------------------|----------------------|-------------------|
| delta_<br/><таблица>                           | Данные из исходной системы                               | X    | X     | X                | X                    | X                 |
| ext_<br/><таблица>                             | Внешняя таблица к исходной системе                       | X    | X     | X                | X                    | X                 |
| bkp_<br/><таблица>                             | Бэкап данных целевой таблицы                             | X    |       |                  |                      |                   |
| buffer_<br/><таблица>                          | Таблица для обмена с дефолтной партицией целевой таблицы |      |       | X                |                      |                   |
| prt_<br/><таблица>_<br/><дата начала партиции> | Таблица для обмена с партицией целевой таблицы           |      |       |                  | X                    |                   |
