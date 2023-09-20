# ETL-framework

## Содержимое
Репозиторий содержит исходные коды функционала по загрузке данных в Greenplum из внешних систем и последующего преобразования данных

## Порядок установки
1. Создайте схему для хранения фреймворка (schemas/create.sql)
2. Создайте таблицы из схемы fw, начиная с таблиц-словарей d_* (schemas/fw/tables)
2. Создайте последовательности из схемы fw (schemas/fw/sequences)
3. Создайте Функции из схемы fw (schemas/fw/functions)
4. Установите расширения postgres_fdw и dblink (settings/extensions/create.sql)
5. Настройте сервер postgres_fdw (settings/fdw/create.sql)

Фреймворк готов к работе!

## License
Apache 2.0

## Contacts
info@sapiens.solutions