INSERT INTO ${target_schema}.d_load_method (load_method, desc_short, desc_middle, desc_long) VALUES
	('dblink', 'Загрузка через dblink через мастер-ноду  ', 'Load from dblink', NULL),
	('function', 'Загрузка функцией', 'Load data from function', NULL),
	('gpfdist', 'Загрузка через gpfdist', 'Load from gpfdist and external table from python module', NULL),
	('odata', 'Загрузка через ODATA ', 'Load from ODATA framework', NULL),
	('pxf', 'Загрузка через PXF', 'Load from pxf and external table', NULL),
	('pxf_ch', 'Выгрузка данных в Clickhouse через PXF', 'Выгрузка данных через PXF во внешние системы', NULL),
	('python', 'Загрузка через python', 'Load from python directly into table', NULL);
