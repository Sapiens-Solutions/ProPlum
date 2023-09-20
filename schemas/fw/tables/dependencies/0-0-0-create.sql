-- fw.dependencies definition

-- Drop table

-- DROP TABLE fw.dependencies;

CREATE TABLE fw.dependencies (
	object_id int8 NULL,
	object_id_depend int8 NULL,
	CONSTRAINT fk_object_id FOREIGN KEY (object_id) REFERENCES fw.objects(object_id),
	CONSTRAINT fk_object_id_depend FOREIGN KEY (object_id_depend) REFERENCES fw.objects(object_id)
)
DISTRIBUTED REPLICATED;

-- Permissions

ALTER TABLE fw.dependencies OWNER TO "admin";
GRANT ALL ON TABLE fw.dependencies TO "admin";