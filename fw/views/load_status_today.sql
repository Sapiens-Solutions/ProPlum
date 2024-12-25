CREATE OR REPLACE VIEW ${target_schema}.load_status_today
AS SELECT o.load_group,
    o.object_id AS "Object Id",
    o.object_name AS "Object name",
    o.object_desc AS "Description",
    ls.load_status AS "Load status",
    li.row_cnt AS "Rows affected",
    timezone('Europe/Moscow'::text, timezone('UTC'::text, li.updated_dttm)) AS "Last update"
   FROM ${target_schema}.objects o
     LEFT JOIN ${target_schema}.load_info li ON o.object_id = li.object_id AND li.updated_dttm::date = 'now'::text::date
     LEFT JOIN ${target_schema}.d_load_status ls ON li.load_status = ls.load_status
  WHERE (o.load_group IN ( SELECT d_load_group.load_group
           FROM ${target_schema}.d_load_group)) AND o.active
  ORDER BY o.object_id;

-- Permissions

ALTER TABLE ${target_schema}.load_status_today OWNER TO "${owner}";
GRANT ALL ON TABLE ${target_schema}.load_status_today TO "${owner}";