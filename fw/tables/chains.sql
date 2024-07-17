create table ${target_schema}.chains
(
chain_name text,
chain_description text,
active bool,
schedule text,
job_name text,
sequence text,
CONSTRAINT pk_chain_name PRIMARY KEY (chain_name)
)
DISTRIBUTED REPLICATED;
COMMENT ON TABLE ${target_schema}.chains IS 'Process chains';
COMMENT ON COLUMN ${target_schema}.chains.chain_name IS 'Name of process chain';
COMMENT ON COLUMN ${target_schema}.chains.chain_description IS 'Process chain description';
COMMENT ON COLUMN ${target_schema}.chains.active IS 'Active flag';
COMMENT ON COLUMN ${target_schema}.chains.schedule IS 'Chain schedule';
COMMENT ON COLUMN ${target_schema}.chains.job_name IS 'Airflow DAG name';
COMMENT ON COLUMN ${target_schema}.chains.sequence IS 'Steps in DAG';

alter table ${target_schema}.chains owner to "${owner}";
grant all on table ${target_schema}.chains to "${owner}";