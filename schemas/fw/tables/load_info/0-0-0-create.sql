-- fw.load_info definition

-- Drop table

-- DROP TABLE fw.load_info;

CREATE TABLE fw.load_info (
	load_id int8 NOT NULL,
	load_status int4 NOT NULL,
	object_id int8 NOT NULL,
	extraction_type text NULL,
	load_type text NULL,
	extraction_from timestamp NULL,
	extraction_to timestamp NULL,
	load_from timestamp NULL,
	load_to timestamp NULL,
	load_method text NULL,
	job_name text NULL,
	created_dttm timestamp NOT NULL DEFAULT now(),
	updated_dttm timestamp NOT NULL DEFAULT now(),
	row_cnt int8 NULL,
	CONSTRAINT fk_extraction_type FOREIGN KEY (extraction_type) REFERENCES fw.d_extraction_type(extraction_type),
	CONSTRAINT fk_load_method FOREIGN KEY (load_method) REFERENCES fw.d_load_method(load_method),
	CONSTRAINT fk_load_status FOREIGN KEY (load_status) REFERENCES fw.d_load_status(load_status),
	CONSTRAINT fk_load_type FOREIGN KEY (load_type) REFERENCES fw.d_load_type(load_type),
	CONSTRAINT fk_object_id FOREIGN KEY (object_id) REFERENCES fw.objects(object_id)
)
DISTRIBUTED BY (load_id);

-- Permissions

ALTER TABLE fw.load_info OWNER TO "admin";
GRANT ALL ON TABLE fw.load_info TO "admin";