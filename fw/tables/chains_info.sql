create table ${target_schema}.chains_info 
(
instance_id int8,
chain_name text,
load_from timestamp,
load_to timestamp,
status int,
chain_start timestamp,
chain_finish timestamp,
	CONSTRAINT pk_instance_id PRIMARY KEY (instance_id),
	CONSTRAINT fk_chain_name FOREIGN KEY (chain_name) REFERENCES ${target_schema}.chains(chain_name),
	CONSTRAINT fk_load_status FOREIGN KEY (status) REFERENCES ${target_schema}.d_load_status(load_status)
)
DISTRIBUTED by (instance_id);
COMMENT ON TABLE ${target_schema}.chains_info IS 'Process chains runs info';

COMMENT ON COLUMN ${target_schema}.chains_info.chain_name IS 'Process chain name';
COMMENT ON COLUMN ${target_schema}.chains_info.instance_id IS 'Instance id of process chain';
COMMENT ON COLUMN ${target_schema}.chains_info.load_from IS 'load_id.extraction_from parameter';
COMMENT ON COLUMN ${target_schema}.chains_info.load_to IS 'load_id.extraction_to parameter';
COMMENT ON COLUMN ${target_schema}.chains_info.status IS 'Process chain status';
COMMENT ON COLUMN ${target_schema}.chains_info.chain_start IS 'Process chain start';
COMMENT ON COLUMN ${target_schema}.chains_info.chain_finish IS 'Process chain finish';

alter table ${target_schema}.chains_info owner to "${owner}";
grant all on table ${target_schema}.chains_info to "${owner}";