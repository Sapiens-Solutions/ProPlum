import psycopg2
import argparse
from pathlib import Path
import os
import yaml
from collections import OrderedDict

# to export objects ddl from schema, run command below
# python C:\downloads\ddl_export.py --host=hostname --db=dbname --user=username --password=password --dbschema=schemaname --outdir="C:\BitBucket\adwh\schemas\schemaname"


def export_schema(cur, schema_name, out_dir, params):
    sql = f"""
        select
            nspname,
            rolname as nspowner,
            'CREATE SCHEMA ' || nspname || ' AUTHORIZATION "' || r.rolname || E'";\n' ||
            E'\n-- Permissions\n\n' ||
            'GRANT ALL ON SCHEMA ' || nspname || ' TO "' || rolname || E'";' as create_sql
        from pg_namespace ns
        join pg_roles r
            on ns.nspowner = r.oid
        where nspname = '{schema_name}';
        """
    cur.execute(sql)
    rec = cur.fetchone()    
    print('Exporting schema ' + schema_name)
    f = open(out_dir + '\\schema.sql', mode='w+', newline='\r\n', encoding='utf-8')    
    f.write(
        rec[2].replace('SCHEMA ' + schema_name, 'SCHEMA ${target_schema}').replace('"' + rec[1] + '"', '"${owner}"') if params is True else rec[2]
    )
    f.close()


def export_sequences(cur, schema_name, out_dir, params):
    sql = f"""
        select
            sequence_schema,
            sequence_name,
            rolname as sequence_owner,
            'CREATE SEQUENCE ' || sequence_schema || '.' || sequence_name || E'\n' ||
            E'\tINCREMENT BY ' || seq.increment::text || E'\n' ||
            E'\tMINVALUE ' || seq.minimum_value::text || E'\n' ||
            E'\tMAXVALUE ' || seq.maximum_value::text || E'\n' ||
            E'\tSTART ' || seq.start_value::text || E'\n' ||
            E'\t' || case seq.cycle_option when 'NO' then 'NO CYCLE' else 'CYCLE' end || E';\n' ||
            E'\n-- Permissions\n\n' ||
            'ALTER SEQUENCE ' || sequence_schema || '.' || sequence_name || ' OWNER TO "' || rolname || E'";\n' 
            'GRANT ALL ON SEQUENCE ' || sequence_schema || '.' || sequence_name || ' TO "' || rolname || E'";' as create_sql
        from pg_class cl 
        join pg_catalog.pg_namespace ns 
            on cl.relnamespace = ns.oid
        join information_schema.sequences seq
            on ns.nspname = sequence_schema
            and cl.relname = sequence_name
        join pg_roles rol 
            on cl.relowner = rol.oid
        where sequence_schema = '{schema_name}';
        """

    cur.execute(sql)
    for rec in cur.fetchall():
        print('Exporting sequence ' + rec[1])
        f = open(out_dir + '\\sequences\\' + rec[1] + '.sql', mode='w+', newline='\r\n', encoding='utf-8')
        f.write(
            rec[3].replace(schema_name + '.', '${target_schema}.').replace('TO "' + rec[2] + '"', 'TO "${owner}"') if params is True else rec[3]
        )
        f.close()


def export_functions(cur, schema_name, out_dir, params):
    sql = f"""
        select 
            nspname,
            proname || '(' || coalesce(param_types, '') || ')' as funcname,
            rolname,
            'CREATE OR REPLACE FUNCTION ' || nspname || '.' || proname || '(' || params || E')\n' ||
            E'\tRETURNS ' || 
            case
                when rettable is not null and rettable != '' then 'TABLE (' || rettable || E')\n'
                else
                    case proretset when true then 'SETOF ' else '' end || 
                    case rettypns when 'pg_catalog' then '' else rettypns || '.' end || rettypname || E'\n' 
            end || 
            E'\tLANGUAGE ' || lanname || E'\n' ||
            case prosecdef when true then E'\tSECURITY DEFINER\n' else '' end ||
            case provolatile 
                when 'i' then E'\tIMMUTABLE\n'
                when 's' then E'\tSTABLE\n'
                when 'v' then E'\tVOLATILE\n'
                else ''
            end ||
            E'AS $$' || prosrc || E'$$\n' ||
            case proexeclocation 
                when 'm' then E'EXECUTE ON MASTER;\n'
                when 'a' then E'EXECUTE ON ANY;\n'
                when 's' then E'EXECUTE ON ALL SEGMENTS;\n'
                when 'i' then E'EXECUTE ON INITPLAN;\n'
                else ''
            end ||
                E'\n-- Permissions\n\n' ||
                'ALTER FUNCTION ' || nspname || '.' || proname || '(' || coalesce(replace(param_types, ',', ', '), '') || ') OWNER TO "' || rolname || E'";\n' ||
                'GRANT ALL ON FUNCTION ' || nspname || '.' || proname || '(' || coalesce(replace(param_types, ',', ', '), '') || E') TO public;\n' || 
                'GRANT ALL ON FUNCTION ' || nspname || '.' || proname || '(' || coalesce(replace(param_types, ',', ', '), '') ||  ') TO "' || rolname || E'";\n' 
            as create_sql
        from (
            select
                nspname,
                proname,
                rolname,
                proretset,
                rettypname,
                rettypns,
                lanname,
                prosecdef,
                provolatile,
                prosrc,
                proexeclocation,
                string_agg(typname, ',' order by param_num) filter (where param_mode != 't') as param_types,
                string_agg(
                    case param_mode
                        when 'i' then ''
                        when 'o' then 'out '
                        when 'b' then 'inout '
                        when 'v' then 'variadic '
                        else ''
                    end ||
                    case param_name when '' then '' else param_name || ' ' end ||
                    coalesce(typname, '') ||
                    case default_value 
                        when '' then '' 
                        else ' DEFAULT ' || default_value
                    end,
                    ', ' order by param_num
                ) filter (where param_mode != 't') as params,
                string_agg(
                    case param_name when '' then '' else param_name || ' ' end || coalesce(typname, ''),
                    ', ' order by param_num
                ) filter (where param_mode = 't') as rettable
            from (
                select
                    ns.nspname,
                    proname,
                    rolname,
                    proretset,
                    typname as rettypname,
                    rettypns.nspname as rettypns,
                    lanname,
                    prosecdef,
                    provolatile,
                    prosrc,
                    proexeclocation,
                    unnest(coalesce(proargmodes, array_fill(''::text, array[1]))) as param_mode,
                    case pronargs when 0 then unnest(array[0]) else generate_subscripts(proargnames, 1) end as param_num,
                    case pronargs when 0 then unnest(array['']) else unnest(proargnames) end as param_name,
                    case pronargs when 0 then unnest(array[null]) else unnest(string_to_array(proargtypes::text, ' ')) end as param_type_oid,
                    case
                        when pronargs = 0 then unnest(array[''])
                        when pronargs > 0 and pronargdefaults = 0 then unnest(array_fill(''::text, array[pronargs]))
                        else unnest(array_fill(''::text, array[pronargs - pronargdefaults]) || string_to_array(pg_get_expr(proargdefaults, 'pg_catalog.pg_proc'::regclass), ', ') ) 
                    end as default_value
                from pg_proc as proc
                join pg_language as lang on proc.prolang = lang.oid
                join pg_namespace as ns on proc.pronamespace = ns.oid
                join pg_type as rettyp on proc.prorettype::oid = rettyp.oid
                join pg_roles as rol on proc.proowner::oid = rol.oid
                join pg_namespace as rettypns on rettyp.typnamespace = rettypns.oid
            ) params
            left join pg_type as typ on params.param_type_oid::oid = typ.oid
            group by 1,2,3,4,5,6,7,8,9,10,11
        ) as query
        where nspname = '{schema_name}'
        order by 1,2
        """

    cur.execute(sql)
    for rec in cur.fetchall():
        print('Exporting function ' + rec[1])
        f = open(out_dir + '\\functions\\' + rec[1] + '.sql', mode='w+', newline='\r\n', encoding='utf-8')
        f.write(
            rec[3].replace(schema_name + '.', '${target_schema}.').replace('TO "' + rec[2] + '"', 'TO "${owner}"') if params is True else rec[3]
        )
        f.close()


def export_tables(cur, schema_name, out_dir, params):
    sql = f"""
    with attr as (
        select
            replace(attrelid::regclass::text,'"','') as tabname,
            'CREATE TABLE ' || attrelid::regclass || E' (\n' ||
            string_agg(
                E'\t' || 
                attname || ' ' || 
                t.typname || 
                case a.attnotnull when true then ' NOT NULL' else ' NULL' end ||
                case when d.adbin is null then '' else ' DEFAULT ' || pg_get_expr(d.adbin, d.adrelid) end ,
                E',\n' order by attnum
            ) as attr
        from pg_attribute a 
        join pg_type t 
            on atttypid = t.oid
        left join pg_catalog.pg_attrdef d 
            on (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
        where 
            not a.attisdropped
            and a.attnum > 0
            and replace(attrelid::regclass::text,'"','') like '{schema_name + '.%'}'
        group by attrelid::regclass
    ),
    constr as (
        select 
            tabname,
            string_agg(constr, E',\n' order by contype desc, conname asc) || E'\n' as constr
        from (
            select
                replace(conrelid::regclass::text ,'"','') as tabname,
                conname,
                contype,
                case contype
                    when 'p' then E'\tCONSTRAINT ' || conname || ' PRIMARY KEY (' || string_agg(a1.attname, ', ') || ')'
                    when 'f' then E'\tCONSTRAINT ' || conname || ' FOREIGN KEY (' || string_agg(a1.attname, ', ') || ') REFERENCES ' || confrelid::regclass || '(' || string_agg(a2.attname, ', ') || ')'
                    else '!!!CONSTRAINT TYPE NOT SUPPORTED!!!'
                end as constr
            from (
                select
                    conrelid,
                    conname,
                    contype,
                    confrelid,
                    unnest(coalesce(conkey, array[0])) as conkey,
                    unnest(coalesce(confkey, array[0])) as confkey
                from pg_constraint
            ) as c
            left join pg_attribute a1
                on c.conrelid = a1.attrelid
                and c.conkey = a1.attnum
            left join pg_attribute a2
                on c.confrelid = a2.attrelid
                and c.confkey = a2.attnum
            group by conrelid::regclass, conname, contype, confrelid::regclass
            order by contype desc, conname asc
        ) as constr
        group by tabname
    ),
    dist as (
        select
            tabname,
            E')\nDISTRIBUTED ' ||
            case 
                when policytype = 'p' and attname is not null then 'BY (' || string_agg(attname, ', ') || E')\n'
                when policytype = 'p' and attname is null then E'RANDOMLY\n'
                when policytype = 'r' then E'REPLICATED\n'
                else '!!!DISTRIBUTION TYPE NOT SUPPOTED!!!'
            end as dist
        from (
            select
                localoid,
                replace(localoid::regclass::text ,'"','') as tabname,
                policytype,
                case when distkey::text = '' then unnest(array[''::text]) else unnest(string_to_array(distkey::text, ' ')) end as distkey
            from gp_distribution_policy
        ) dist
        left join pg_attribute a
            on dist.localoid = a.attrelid
            and dist.distkey = a.attnum::text
        group by tabname, policytype, attname
    ),
    rules as (
        select 
            schemaname || '.' || tablename as tabname,
            E'\n-- Table Rules\n\n' || string_agg(definition, E'\n\n' order by rulename) || E'\n\n'as rules
        from pg_rules
        group by 1
    ),
    perm as (
select a.tabname, rolname, perm || priv as perm from ( 
select
    replace( c.oid::regclass::text,'"','') as tabname,
    rolname,
    E'\n-- Permissions\n\n' ||
    'ALTER TABLE ' || c.oid::regclass || ' OWNER TO "' || rolname || E'";\n' as perm --||
   -- 'GRANT ALL ON TABLE ' || c.oid::regclass || ' TO "' || rolname || E'";' as perm
from pg_class c
left join pg_roles r
    on c.relowner = r.oid          
) as a       
left join 
(
	select tabname, string_agg(priv,'') as priv
	from
	(select table_schema|| '.' || table_name as tabname, 'GRANT '||string_agg(privilege_type,', ') || ' ON TABLE ' || table_schema ||'.'|| table_name || ' TO "' || grantee || E'";\n' as priv  
	           from information_schema.role_table_grants 
	           group by table_schema, table_name ,grantee) as b           
	group by 1 
) as b 
      on a.tabname = b.tabname
    ),
    ddl as (
    	select
	    	dist,
	    	reloptions,
	    	attr.tabname,
	        (string_to_array(attr.tabname, '.'))[1] as schema_name,
	        (string_to_array(attr.tabname, '.'))[2] as table_name,
	        perm.rolname as owner_name,
	        attr || case when constr is null then E'\n' else E',\n' || constr end || E'\n' || coalesce(rules, '') as create_sql,
	        perm,
	        partit
	    from attr 
	    left join constr on attr.tabname = constr.tabname
	    join dist on attr.tabname = dist.tabname
	    left join rules on attr.tabname = rules.tabname
	    join perm on attr.tabname = perm.tabname    
	    left join (select table_schema||'.'||table_name as tabname, pg_get_partition_def((table_schema||'.'||table_name)::regclass, true, false) as partit
		from information_schema.tables) as sub	
			on sub.tabname = attr.tabname    
	left join(
		 select
		 nspname||'.'||relname as sh_tab_name, reloptions FROM pg_catalog.pg_class c 
	    JOIN pg_catalog.pg_namespace n 
		    ON n.oid = c.relnamespace WHERE nspname ILIKE '{schema_name}') as a
		    on a.sh_tab_name = attr.tabname
	where attr.tabname like '{schema_name + '.%'}' 
    and attr.tabname not like '%_prt_%'
    order by attr.tabname)
select schema_name, table_name, owner_name, create_sql || case when reloptions is null then '' else 'WITH ('|| E'\n' || array_to_string(ARRAY(select unnest(array[reloptions])), E',\n') ||E'\n' end || dist || case when partit is null then '' else partit || ';' end || E'\n' || perm  as create_sql  
from ddl
WHERE (create_sql || case when reloptions is null then '' else 'WITH ('|| E'\n' || array_to_string(ARRAY(select unnest(array[reloptions])), E',\n') ||E'\n' end || dist || case when partit is null then '' else partit || ';' end || E'\n' || perm) IS NOT NULL 
    """

    cur.execute(sql)
    for rec in cur.fetchall():
        print('Exporting table ' + rec[1])
        f = open(out_dir + '\\tables\\' + rec[1] + '.sql', mode='w+', newline='\r\n', encoding='utf-8')
        if rec[3]:
            f.write(
            rec[3].replace(schema_name + '.', '${target_schema}.').replace('TO "' + rec[2] + '"', 'TO "${owner}"') if params is True else rec[3]
        )
        f.close()


# export views
def export_views(cur, schema_name, out_dir, params):
    sql = f"""SELECT 
defs.table_schema AS schema_name,
defs.table_name,
perm.rolname AS owner_name,
CASE 
	WHEN defs.view_definition IS NULL THEN v.definition || E'\n' || perm.perm 
	ELSE defs.view_definition || E'\n' || perm.perm 
END AS create_sql						
from information_schema.VIEWS AS "defs"
LEFT JOIN pg_views v ON v.viewname = defs.table_name AND v.schemaname = defs.table_schema
LEFT JOIN (
select b.tabname, rolname, perm || priv as perm from (
	select table_name, tabname, string_agg(priv,'') as priv
	from
	(select table_name, table_schema|| '.' || table_name as tabname, 'GRANT '||string_agg(privilege_type,', ') || ' ON TABLE ' || table_schema ||'.'|| table_name || ' TO "' || grantee || E'";\n' as priv  
	           from information_schema.role_table_grants 
	           group by table_schema, table_name ,grantee) as b           
	group by 1, 2 
) as b 
left join
( 
select
    replace( c.oid::regclass::text,'"','') as tabname,
    rolname,
    E'\n-- Permissions\n\n' ||
    'ALTER TABLE ' || c.oid::regclass || ' OWNER TO "' || rolname || E'";\n' as perm
from pg_class c
left join pg_roles r
    on c.relowner = r.oid 
 WHERE relkind = 'v'
) as a on a.tabname = b.tabname OR a.tabname = b.table_name
      ) AS perm ON perm.tabname = defs.table_schema || '.' || table_name
WHERE table_schema = '{schema_name}'
    """

    cur.execute(sql)
    for rec in cur.fetchall():
        print('Exporting view ' + rec[1])
        f = open(out_dir + '\\views\\' + rec[1] + '.sql', mode='w+', newline='\r\n', encoding='utf-8')
        if rec[3]:
            f.write(
            rec[3].replace(schema_name + '.', '${target_schema}.').replace('TO "' + rec[2] + '"', 'TO "${owner}"') if params is True else rec[3]
        )
        f.close()        


def export_extensions(out_dir):
    print('Exporting extension dblink')
    f = open(out_dir + '\\extensions\\dblink.sql', mode='w+', newline='\r\n', encoding='utf-8')
    f.write('CREATE EXTENSION IF NOT EXISTS dblink;')
    f.close()

    print('Exporting extension postgres_fdw')
    f = open(out_dir + '\\extensions\\postgres_fdw.sql', mode='w+', newline='\r\n', encoding='utf-8')
    f.write('CREATE EXTENSION IF NOT EXISTS postgres_fdw;')
    f.close()


def export_servers(out_dir):
    print('Exporting foreign server')
    f = open(out_dir + '\\servers\\server.sql', mode='w+', newline='\r\n', encoding='utf-8')
    f.write(
        "CREATE SERVER adwh_server\n"
        "        FOREIGN DATA WRAPPER postgres_fdw\n"
        "        OPTIONS (host '<your greenplum server>', port '5432', dbname '<your greenplum database>');\n\n"
        'CREATE USER MAPPING FOR "<your log user>"\n'
        "        SERVER adwh_server\n"
        "        OPTIONS (user 'your log user', password 'password');\n"
    )
    f.close()


def export_table_data(cur, schema_name, out_dir, params):
    sql = f"""
        select
            (string_to_array(attrelid::regclass::text, '.'))[1] as schema_name,
            (string_to_array(attrelid::regclass::text, '.'))[2] as table_name,
            'INSERT INTO ' || attrelid::regclass || ' (' || string_agg(attname, ', ' order by attnum) || E') VALUES' as insert_sql,
            array_agg(t.typname order by attnum)
        from pg_attribute a 
        join pg_type t 
            on atttypid = t.oid
        where 
            not a.attisdropped
            and a.attnum > 0
            and (attrelid::regclass::text like '{schema_name + '.d/_%'}' escape '/' or attrelid::regclass::text = '{schema_name + '.'}load_constants')
        group by attrelid::regclass
        order by attrelid::regclass
        """

    cur.execute(sql)
    recs = cur.fetchall()
    for rec in recs:
        print('Exporting table data for ' + rec[1])
        cur.execute(f"select * from {rec[0] + '.' + rec[1]} order by 1")
        data_recs = cur.fetchall()

        f = open(out_dir + '\\tables\\' + rec[1] + '-data.sql', mode='w+', newline='\r\n', encoding='utf-8')

        if len(data_recs) > 0:
            f.write(
                (rec[2].replace(schema_name + '.', '${target_schema}.') if params is True else rec[2]) + '\n'
            )
            for data_rec in data_recs:
                f.write('\t' + str(data_rec).replace('None', 'NULL'))
                if data_recs[-1] == data_rec:
                    f.write(';\n')
                else:
                    f.write(',\n')
        else:
            print('No data')

        f.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='ddl_export',
        description='Sapiens Greenplum ETL framework export tool',
    )

    parser.add_argument('--host', required=True, help='GP master host')
    parser.add_argument('--port', required=False, help='GP port (default 5432)', default='5432')
    parser.add_argument('--db', required=True, help='GP database name')
    parser.add_argument('--user', required=True, help='User name')
    parser.add_argument('--password', required=True, help='User password')
    parser.add_argument('--dbschema', required=True, help='Framework schema')
    parser.add_argument('--outdir', required=True, help='Framework files output directory')
    parser.add_argument('--params', required=False, help='Schema and owner replaced by parameter placeholders', action='store_true')

    args = parser.parse_args()

    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.db,
        user=args.user,
        password=args.password
    )

    cur = conn.cursor()

    Path(args.outdir + '\\tables').mkdir(parents=True, exist_ok=True)
    Path(args.outdir + '\\functions').mkdir(parents=True, exist_ok=True)
    Path(args.outdir + '\\sequences').mkdir(parents=True, exist_ok=True)
    Path(args.outdir + '\\extensions').mkdir(parents=True, exist_ok=True)
    Path(args.outdir + '\\servers').mkdir(parents=True, exist_ok=True)
    Path(args.outdir + '\\views').mkdir(parents=True, exist_ok=True)

    export_schema(cur, args.dbschema, args.outdir, args.params)
    export_tables(cur, args.dbschema, args.outdir, args.params)
    #export_table_data(cur, args.dbschema, args.outdir, args.params)
    export_functions(cur, args.dbschema, args.outdir, args.params)
    export_sequences(cur, args.dbschema, args.outdir, args.params)
    export_extensions(args.outdir)
    export_servers(args.outdir)
    export_views(cur, args.dbschema, args.outdir, args.params)

    conn.close()


# organize files in directory "0-0-0-create.sql" 
def organize_files_recursively(directory_path):
    for root, dirs, files in os.walk(directory_path):
        for filename in files:
            # full path
            file_path = os.path.join(root, filename)
            
            # create directory
            folder_name = os.path.splitext(filename)[0]
            folder_path = os.path.join(root, folder_name)
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            # move file into directory and rename
            new_file_path = os.path.join(folder_path, "0-0-0-create.sql")
            os.rename(file_path, new_file_path)


organize_files_recursively(args.outdir)


# create yaml
def ordered_dict_presenter(dumper, data):
    return dumper.represent_dict(data.items())

yaml.add_representer(OrderedDict, ordered_dict_presenter)

def list_files_with_changeset(directory_path, author_name):
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.sql'): 
                absolute_path = os.path.abspath(directory_path)
                path_parts = absolute_path.split(os.sep)
                half_path = os.sep.join(path_parts[-3:])             
                relative_path = os.path.relpath(os.path.join(root, file), directory_path)
                relative_path = os.path.join(half_path, relative_path)                
                yield OrderedDict([
                    ('changeSet', OrderedDict([
                        ('id', '0-0-0'),
                        ('author', author_name),
                        ('changes', [
                            OrderedDict([
                                ('sqlFile', OrderedDict([
                                    ('path', relative_path)
                                ]))
                            ])
                        ])
                    ]))
                ])


directory_path = args.outdir  # start path
author_name = ''
output_file = args.outdir + '\\' + args.dbschema + '.yaml' # target file path

# collect into list
changesets = list(list_files_with_changeset(directory_path, author_name))

# write into YAML
with open(output_file, 'w') as yaml_file:
    yaml_file.write('databaseChangeLog:\n')
    yaml.dump(changesets, yaml_file, default_flow_style=False)
    

# check if files.yaml exists
if os.path.exists(output_file):
    print('File {}.yaml created successfuly '.format(args.dbschema))
    print('File path: {}'.format(output_file))
else:
    print('Error while creating {}.yaml'.format(args.dbschema))


# create master.yaml 
def generate_yaml(directory_path):
    # list of paths
    yaml_entries = []

    # get list of directories
    subdirectories = [d for d in os.listdir(os.path.dirname(directory_path)) if os.path.isdir(os.path.join(os.path.dirname(directory_path), d))]

    # create entries in YAML
    for subdir in subdirectories:
        yaml_entry = {
            'include': {
                'file': f"{subdir}/{subdir}.yaml"
            }
        }
        yaml_entries.append(yaml_entry)

    database_change_log = {'databaseChangeLog': yaml_entries}

    yaml_str = yaml.dump(database_change_log, default_flow_style=False, allow_unicode=True)

    # write entry into yaml
    with open(os.path.dirname(directory_path) + '\\master.yaml', 'w', encoding='utf-8') as file:
        file.write(yaml_str)

    return print("master.yaml created successfuly")

generate_yaml(args.outdir)

