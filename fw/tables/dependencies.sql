CREATE TABLE ${target_schema}.dependencies (
	object_id int8 NULL,
	object_id_depend int8 NULL,
	CONSTRAINT fk_object_id FOREIGN KEY (object_id) REFERENCES ${target_schema}.objects(object_id),
	CONSTRAINT fk_object_id_depend FOREIGN KEY (object_id_depend) REFERENCES ${target_schema}.objects(object_id)
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE ${target_schema}.dependencies OWNER TO "${owner}";
GRANT ALL ON TABLE ${target_schema}.dependencies TO "${owner}";