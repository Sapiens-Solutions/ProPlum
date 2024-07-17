create table ${target_schema}.chains_log 
(
log_id int8,
instance_id int8,
log_timestamp timestamp,
log_type text,
log_msg text,
	CONSTRAINT pk_chain_log_id PRIMARY KEY (log_id),
	CONSTRAINT fk_instance_id FOREIGN KEY (instance_id) REFERENCES ${target_schema}.chains_info(instance_id)
)
DISTRIBUTED by (log_id);
COMMENT ON TABLE ${target_schema}.chains_log IS 'Detailed log of process chain';
COMMENT ON COLUMN ${target_schema}.chains_log.log_id IS 'Log id';
COMMENT ON COLUMN ${target_schema}.chains_log.instance_id IS 'Process chain instance id';
COMMENT ON COLUMN ${target_schema}.chains_log.log_timestamp IS 'Message timestamp';
COMMENT ON COLUMN ${target_schema}.chains_log.log_type IS 'Message type';
COMMENT ON COLUMN ${target_schema}.chains_log.log_msg IS 'Log message';

alter table ${target_schema}.chains_log owner to "${owner}";
grant all on table ${target_schema}.chains_log to "${owner}";