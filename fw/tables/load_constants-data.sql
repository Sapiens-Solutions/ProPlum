INSERT INTO ${target_schema}.load_constants (constant_name, constant_type, constant_value) VALUES
	('c_bkp_table_prefix', 'text', 'bkp_'),
	('c_buf_table_prefix', 'text', 'buffer_'),
	('c_delta_table_prefix', 'text', 'delta_'),
	('c_ext_table_prefix', 'text', 'ext_'),
	('c_log_fdw_server', 'text', 'adwh_server'),
	('c_stg_table_schema', 'text', 'stg_');
