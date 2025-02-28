# ETL Framework Function Reference

This document provides a detailed reference for calling various functions in the ETL framework. Each function is described along with its parameters, return values, and an example of how to call it.

# ETL Framework Function Reference

## Table of Contents
- [fw.f_analyze_table](#fwf_analyze_table)
- [fw.f_create_date_partitions](#fwf_create_date_partitions)
- [fw.f_create_ext_table](#fwf_create_ext_table)
- [fw.f_create_tmp_table](#fwf_create_tmp_table)
- [fw.f_delete_load_lock](#fwf_delete_load_lock)
- [fw.f_gen_load_id](#fwf_gen_load_id)
- [fw.f_gen_group_load_id](#fwf_gen_group_load_id)
- [fw.f_get_connection_string](#fwf_get_connection_string)
- [fw.f_get_distribution_key](#fwf_get_distribution_key)
- [fw.f_get_enum_partition](#fwf_get_enum_partition)
- [fw.f_get_load_id](#fwf_get_load_id)
- [fw.f_get_load_expression](#fwf_get_load_expression)
- [fw.f_get_locks](#fwf_get_locks)
- [fw.f_get_load_locks](#fwf_get_load_locks)
- [fw.f_get_merge_key](#fwf_get_merge_key)
- [fw.f_get_partition_key](#fwf_get_partition_key)
- [fw.f_get_table_attributes](#fwf_get_table_attributes)
- [fw.f_get_table_schema](#fwf_get_table_schema)
- [fw.f_get_where_clause](#fwf_get_where_clause)
- [fw.f_grant_select](#fwf_grant_select)
- [fw.f_insert_table](#fwf_insert_table)
- [fw.f_load_id_exists](#fwf_load_id_exists)
- [fw.f_load_delta_merge](#fwf_load_delta_merge)
- [fw.f_merge_tables](#fwf_merge_tables)
- [fw.f_partition_name_by_value](#fwf_partition_name_by_value)
- [fw.f_partition_name_list_by_date](#fwf_partition_name_list_by_date)
- [fw.f_prepare_load](#fwf_prepare_load)
- [fw.f_set_load_id_error](#fwf_set_load_id_error)
- [fw.f_set_load_id_in_process](#fwf_set_load_id_in_process)
- [fw.f_set_load_id_success](#fwf_set_load_id_success)
- [fw.f_set_load_lock](#fwf_set_load_lock)
- [fw.f_stat_activity](#fwf_stat_activity)
- [fw.f_switch_def_partition](#fwf_switch_def_partition)
- [fw.f_switch_partition(text,text,text)](#fwf_switch_partitiontexttexttext)
- [fw.f_switch_partition(text,timestamp,text)](#fwf_switch_partitiontexttimestamptext)
- [fw.f_unify_name](#fwf_unify_name)
- [fw.f_upsert_table](#fwf_upsert_table)
- [fw.f_wait_locks](#fwf_wait_locks)
- [fw.f_write_log](#fwf_write_log)
- [fw.f_truncate_table](#fwf_truncate_table)

---

### fw.f_analyze_table
Collects statistics for the specified table.

**Parameters:**
- `p_table_name`: Name of the table with schema (required).

**Example Call:**
```sql
SELECT fw.f_analyze_table(p_table_name := 'src_hybris.addresses');
```

---

### fw.f_create_date_partitions
Creates new date partitions in the specified table if they do not already exist.

**Parameters:**
- `p_table_name`: Name of the table with schema (required).
- `p_partition_value`: Date for which the partition needs to be created (required).

**Example Call:**
```sql
SELECT fw.f_create_date_partitions(p_table_name := 'src_hybris.carts', p_partition_value := '2022-01-01'::timestamp);
```

---

### fw.f_create_ext_table
Creates an external table based on the structure of the specified table.

**Parameters:**
- `p_table_name`: Name of the table with schema (required).
- `p_load_method`: Protocol for creating the external table (e.g., 'pxf', 'gpfdist') (required).
- `p_connect_string`: Connection string to the source (required).
- `p_schema_name`: Schema where the external table will be created (optional).
- `p_prefix`: Prefix for the created table (optional).
- `p_suffix`: Suffix for the created table (optional).

**Example Call:**
```sql
SELECT fw.f_create_ext_table(
    p_table_name := 'src_hybris.addresses',
    p_load_id := 1122,
    p_load_method := 'pxf',
    p_connect_string := 'HYBRISAPP.ADDRESSES?PROFILE=Jdbc&server=hybris',
    p_schema_name := 'stg_hybris',
    p_prefix := 'ext_',
    p_suffix := '_ext'
);
```

---

### fw.f_create_tmp_table
Creates a temporary table used in the ETL process.

**Parameters:**
- `p_table_name`: Name of the table with schema (required).
- `p_schema_name`: Schema where the temporary table will be created (optional).
- `p_prefix_name`: Prefix for the created table (optional).
- `p_suffix_name`: Suffix for the created table (optional).
- `p_drop_table`: Flag indicating whether to drop the existing table before creation (optional, default is true).
- `p_is_temporary`: Flag indicating whether to create a temporary table (optional, default is false).

**Example Call:**
```sql
SELECT fw.f_create_tmp_table(
    p_table_name := 'src_hybris.addresses',
    p_schema_name := 'stg_hybris',
    p_prefix := 'delta_',
    p_drop_table := true,
    p_is_temporary := false
);
```

---

### fw.f_delete_load_lock
Deletes ETL process locks set by the framework.

**Parameters:**
- `p_load_id`: ID of the load from the `fw.load_info` table (optional).
- `p_object_name`: Name of the table with schema (optional).

**Example Call:**
```sql
SELECT fw.f_delete_load_lock(p_load_id := 12342);
```

---

### fw.f_gen_load_id
Generates a load ID for a specified object.

**Parameters:**
- `p_object_id`: Identifier of the object from the `fw.objects.object_id` table (required).
- Additional optional parameters include `p_start_extr`, `p_end_extr`, `p_extraction_type`, `p_load_type`, etc.

**Example Call:**
```sql
SELECT fw.f_gen_load_id(
    p_object_id := 25,
    p_start_extr := '2022-01-01',
    p_end_extr := '2022-02-01',
    p_extraction_type := 'FULL',
    p_load_type := 'DELTA_UPSERT'
);
```

---

### fw.f_gen_group_load_id
Generates a set of load IDs for a group of loads.

**Parameters:**
- `p_load_group`: Group of loads from the `fw.objects.load_group` table (required).
- Additional optional parameters similar to `f_gen_load_id`.

**Example Call:**
```sql
SELECT fw.f_gen_group_load_id(
    p_load_group := 'SRC_HYBRIS',
    p_start_extr := '2022-01-01',
    p_end_extr := '2022-02-01',
    p_extraction_type := 'DELTA',
    p_load_type := NULL
);
```

---

### fw.f_get_connection_string
Retrieves the connection string to an external system.

**Parameters:**
- `p_load_id`: ID of the load (required).

**Example Call:**
```sql
SELECT fw.f_get_connection_string(p_load_id := 1234);
```

---

### fw.f_get_distribution_key
Determines the distribution key for a table.

**Parameters:**
- `p_table_name`: Name of the table with schema (required).

**Example Call:**
```sql
SELECT fw.f_get_distribution_key(p_table_name := 'src_hybris.addresses');
```

---

### fw.f_get_enum_partition
Retrieves PXF partitions for individual values.

**Parameters:**
- `p_field_name`: Field name for partitioning (required).
- `p_format`: Format for date conversion (required).
- `p_start_date`: Start date for partitioning interval (required).
- `p_end_date`: End date for partitioning interval (required).

**Example Call:**
```sql
SELECT fw.f_get_enum_partition(
    p_field_name := '"CALDAY"',
    p_format := 'YYYYMMDD',
    p_start_date := '2023-01-01',
    p_end_date := '2023-02-01'
);
```

---

### fw.f_get_load_id
Retrieves the load ID for an object within specified dates.

**Parameters:**
- `p_object_id`: Identifier of the object from the `fw.objects.object_id` table (required).
- `p_start_date`: Start date of the period (optional).
- `p_end_date`: End date of the period (optional).

**Example Call:**
```sql
SELECT fw.f_get_load_id(p_object_id := 25);
```

---

### fw.f_get_load_expression
Retrieves the SQL expression for loading data into staging.

**Parameters:**
- `p_load_id`: ID of the load (required).

**Example Call:**
```sql
SELECT fw.f_get_load_expression(p_load_id := 25202);
```

---

### fw.f_get_locks
Retrieves all locks present in the database.

**Parameters:** None

**Example Call:**
```sql
SELECT fw.f_get_load_locks();
```

---

### fw.f_get_load_locks
Retrieves ETL locks present in the database for a specified table.

**Parameters:**
- `p_object_name`: Name of the table for lock search (optional).

**Example Call:**
```sql
SELECT fw.f_get_load_locks(p_object_name := 'src_hybris.glabels');
```

---

### fw.f_get_merge_key
Retrieves the merge key for an object based on its ID.

**Parameters:**
- `p_object_id`: ID of the object (required).

**Example Call:**
```sql
SELECT fw.f_get_merge_key(p_object_id := 24);
```

---

### fw.f_get_partition_key
Retrieves the partitioning field for a specified table.

**Parameters:**
- `p_table_name`: Name of the table with schema (required).

**Example Call:**
```sql
SELECT fw.f_get_partition_key(p_table_name := 'src_hybris.customertracking');
```

---

### fw.f_get_table_attributes
Retrieves storage attributes for a specified table.

**Parameters:**
- `p_table_name`: Name of the table with schema (required).

**Example Call:**
```sql
SELECT fw.f_get_table_attributes(p_table_name := 'src_hybris.customertracking');
```

---

### fw.f_get_table_schema
Retrieves the schema of a specified table.

**Parameters:**
- `p_table`: Name of the table with schema (required).

**Example Call:**
```sql
SELECT fw.f_get_table_schema(p_table := 'src_hybris.customertracking');
```

---

### fw.f_get_where_clause
Retrieves the WHERE clause for loading data from an external system into the target table for a specified object ID.

**Parameters:**
- `p_object_id`: ID of the object (required).

**Example Call:**
```sql
SELECT fw.f_get_where_clause(p_object_id := 25);
```

---

### fw.f_grant_select
Grants SELECT permissions on a target table based on a template table.

**Parameters:**
- `p_trg_table_name`: Name of the target table with schema (required).
- `p_src_table`: Name of the template table with schema (required).

**Example Call:**
```sql
SELECT fw.f_grant_select(
    p_trg_table_name := 'stg_hybris.delta_customertracking',
    p_src_table := 'src_hybris.customertracking'
);
```

---

### fw.f_insert_table
Loads data from a source table into a target table.

**Parameters:**
- `p_table_from`: Name of the source table with schema (required).
- `p_table_to`: Name of the target table with schema (required).
- `p_where`: Condition for insertion into the target table (optional).
- `p_truncate_tgt`: Flag to truncate the target table before insertion (optional).

**Example Call:**
```sql
SELECT fw.f_insert_table(
    p_table_from := 'stg_hybris.ext_customertracking',
    p_table_to := 'stg_hybris.delta_customertracking',
    p_where := 'p_ajax not in (1)'
);
```

---

### fw.f_load_id_exists
Checks the existence of a load ID within a specified time period.

**Parameters:**
- `p_object_id`: Identifier of the object from the `fw.objects.object_id` table (required).
- `p_start_date`: Start date of the period (required).
- `p_end_date`: End date of the period (required).

**Example Call:**
```sql
SELECT fw.f_load_id_exists(
    p_object_id := 25,
    p_start_date := '2022-01-01'::timestamp,
    p_end_date := '2022-02-01'::timestamp
);
```

---

### fw.f_load_delta_merge
Performs a DELTA_MERGE load by merging tables and replacing the default partition.

**Parameters:**
- `p_load_id`: Load ID from the `fw.load_info.load_id` table (required).

**Example Call:**
```sql
SELECT fw.f_load_delta_merge(p_load_id := 1234);
```

---

### fw.f_merge_tables
Merges two tables using a specified merge key.

**Parameters:**
- `p_table_from_name`: Name of the source table with schema (required).
- `p_table_to_name`: Name of the target table with schema (required).
- `p_where`: WHERE condition applied to the source table (required).
- `p_merge_key`: Merge key for the tables (required).
- `p_trg_table`: Name of the new target table for the merge result (optional).

**Example Call:**
```sql
SELECT fw.f_merge_tables(
    p_table_from_name := 'stg_hybris.delta_customertracking',
    p_table_to_name := 'src_hybris.customertracking',
    p_where := '1=1',
    p_merge_key := array['pk'],
    p_trg_table := 'stg_hybris.prt_customertracking'
);
```

---

### fw.f_partition_name_by_value
Retrieves the partition name based on a date value.

**Parameters:**
- `p_table_name`: Name of the table with schema (required).
- `p_partition_value`: Value to search for (required).

**Example Call:**
```sql
SELECT fw.f_partition_name_by_value(
    p_table_name := 'src_hybris.customertracking',
    p_partition_value := '2022-01-01'::timestamp
);
```

---

### fw.f_partition_name_list_by_date
Retrieves partition names for a specified date range.

**Parameters:**
- `p_table_name`: Name of the table with schema (required).
- `p_partition_start`: Start of the date range (required).
- `p_partition_end`: End of the date range (required).

**Example Call:**
```sql
SELECT fw.f_partition_name_list_by_date(
    p_table_name := 'src_hybris.customertracking',
    p_partition_start := '2022-01-01'::timestamp,
    p_partition_end := '2023-01-01'::timestamp
);
```

---

### fw.f_prepare_load
Prepares for loading data into the target table by creating temporary objects.

**Parameters:**
- `p_load_id`: Load ID from the `fw.load_info` table (required).

**Example Call:**
```sql
SELECT fw.f_prepare_load(p_load_id := 2116);
```

---

### fw.f_set_load_id_error
Sets the status of a load ID to "Error".

**Parameters:**
- `p_load_id`: Load ID from the `fw.load_info` table (required).

**Example Call:**
```sql
SELECT fw.f_set_load_id_error(p_load_id := 2116);
```

---

### fw.f_set_load_id_in_process
Sets the status of a load ID to "In Process".

**Parameters:**
- `p_load_id`: Load ID from the `fw.load_info` table (required).

**Example Call:**
```sql
SELECT fw.f_set_load_id_in_process(p_load_id := 2116);
```

---

### fw.f_set_load_id_success
Sets the status of a load ID to "Success".

**Parameters:**
- `p_load_id`: Load ID from the `fw.load_info` table (required).

**Example Call:**
```sql
SELECT fw.f_set_load_id_success(p_load_id := 2116);
```

---

### fw.f_set_load_lock
Sets an ETL lock on a table for a specified load ID.

**Parameters:**
- `p_load_id`: Load ID from the `fw.load_info` table (required).
- `p_lock_type`: Type of lock (required).
- `p_object_name`: Name of the table (required).

**Example Call:**
```sql
SELECT fw.f_set_load_lock(
    p_load_id := 1234,
    p_lock_type := 'EXCLUSIVE',
    p_object_name := 'src_hybris.carts'
);
```

---

### fw.f_stat_activity
Retrieves current processes in the database.

**Parameters:** None

**Example Call:**
```sql
SELECT fw.f_stat_activity();
```

---

### fw.f_switch_def_partition
Replaces the default partition of a table.

**Parameters:**
- `p_table_from_name`: Name of the source table with schema (required).
- `p_table_to_name`: Name of the target table with schema (required).

**Example Call:**
```sql
SELECT fw.f_switch_def_partition(
    p_table_from_name := 'stg_hybris.buffer_customertracking',
    p_table_to_name := 'src_hybris.customertracking'
);
```

---

### fw.f_switch_partition(text,text,text)
Replaces a specified partition of a table.

**Parameters:**
- `p_table_name`: Name of the target table with schema (required).
- `p_partition_name`: Name of the partition in the target table (required).
- `p_switch_table_name`: Name of the source table with schema (required).

**Example Call:**
```sql
SELECT fw.f_switch_partition(
    p_table_name := 'src_hybris.customertracking',
    p_partition_name := 'customertracking_1_prt_6',
    p_switch_table_name := 'stg_hybris.prt_customertracking'
);
```

---

### fw.f_switch_partition(text,timestamp,text)
Replaces a partition for a specified date value.

**Parameters:**
- `p_table_name`: Name of the target table with schema (required).
- `p_partition_value`: Date value in the partition (required).
- `p_switch_table_name`: Name of the source table with schema (required).

**Example Call:**
```sql
SELECT fw.f_switch_partition(
    p_table_name := 'src_hybris.customertracking',
    p_partition_value := '2022-01-01'::timestamp,
    p_switch_table_name := 'stg_hybris.prt_customertracking'
);
```

---

### fw.f_unify_name
Unifies the format of an input parameter name.

**Parameters:**
- `p_name`: Name of the parameter to unify (required).

**Example Call:**
```sql
SELECT fw.f_unify_name(p_name := 'stg_hybris.delta_customertracking');
```

---

### fw.f_upsert_table
Updates data in the target table using the UPSERT method.

**Parameters:**
- `p_load_id`: Load ID from the `fw.load_info` table (required).
- `p_table_from_name`: Name of the source table with schema (required).
- `p_table_to_name`: Name of the target table with schema (required).
- `p_delete_duplicates`: Flag to delete duplicates based on the merge key (optional).
- `p_analyze`: Flag to collect statistics for the table (optional).
- `p_where`: Condition for loading data (optional).

**Example Call:**
```sql
SELECT fw.f_upsert_table(
    p_load_id := 1424,
    p_table_from_name := 'stg_hybris.delta_customertracking',
    p_table_to_name := 'src_hybris.customertracking',
    p_delete_duplicates := false,
    p_where := '1=1'
);
```

---

### fw.f_wait_locks
Waits for a lock reset on a table using a timer.

**Parameters:**
- `p_table_name`: Name of the source table with schema (required).
- `p_repeat_interval`: Interval to wait in seconds (required).
- `p_repeat_count`: Number of iterations for checking (required).
- `p_terminate_lock`: Flag to terminate the ETL lock on the table (optional).

**Example Call:**
```sql
SELECT fw.f_wait_locks(
    p_table_name := 'src_hybris.customertracking',
    p_repeat_interval := 60,
    p_repeat_count := 60,
    p_terminate_lock := true
);
```

---

### fw.f_write_log
Logs the execution of an operation.

**Parameters:**
- `p_log_type`: Type of log ('ERROR', 'INFO', 'DEBUG', 'WARN') (required).
- `p_log_message`: Message to log (required).
- `p_location`: Function where the message originated (required).
- `p_load_id`: Load ID (optional).

**Example Call:**
```sql
SELECT fw.f_write_log(
    p_log_type := 'INFO',
    p_log_message := 'Start loading table hybris.b2bcustomer from stg_hybris.load_b2bcustomer',
    p_location := 'F_LOAD_B2BCUSTOMER',
    p_load_id := 999
);
```

---

### fw.f_truncate_table
Truncates the specified table.

**Parameters:**
- `p_table_name`: Name of the table to truncate with schema (required).

**Example Call:**
```sql
SELECT fw.f_truncate_table(p_table_name := 'stg_hybris.delta_customertracking');
```

---

### fw.f_load_delta_partitions
Iteratively loads data from one table into partitions of another table.

**Parameters:**
- `p_load_id`: Load ID (required).
- `p_table_from_name`: Name of the source table with schema (required).
- `p_table_to_name`: Name of the target table with schema (required).
- `p_merge_partitions`: Flag to indicate whether to merge changes (optional).
- `p_where`: Condition for loading data (optional).

**Example Call:**
```sql
SELECT fw.f_load_delta_partitions(
    p_load_id := 1501,
    p_table_from_name := 'stg_hybris.delta_customertracking',
    p_table_to_name := 'src_hybris.customertracking',
    p_merge_partitions := false,
    p_where := '1=1'
);
```

---

### fw.f_table_exists
Checks if a table exists in the database.

**Parameters:**
- `p_table_name`: Name of the table with schema (required).

**Example Call:**
```sql
SELECT fw.f_table_exists(p_table_name := 'stg_hybris.delta_customertracking');
```

---

### fw.f_execute_function
Executes a specified function.

**Parameters:**
- `p_function_name`: Name of the function with schema and parameters (required).
- `p_load_id`: Load ID (optional).

**Example Call:**
```sql
SELECT fw.f_execute_function(
    p_function_name := 'hybris.f_load_discounts($load_id)',
    p_load_id := 1495
);
```

---

### fw.f_get_max_value
Retrieves the maximum value of a column.

**Parameters:**
- `p_table_name`: Name of the table (required).
- `p_field_name`: Name of the field in the table (required).
- `p_where`: Condition to limit results (optional).

**Example Call:**
```sql
SELECT fw.f_get_max_value(
    p_table_name := 'stg_hybris.delta_customertracking',
    p_field_name := 'modifiedts',
    p_where := '1=1'
);
```

---

### fw.f_get_min_value
Retrieves the minimum value of a column.

**Parameters:**
- `p_table_name`: Name of the table (required).
- `p_field_name`: Name of the field in the table (required).
- `p_where`: Condition to limit results (optional).

**Example Call:**
```sql
SELECT fw.f_get_min_value(
    p_table_name := 'stg_hybris.delta_customertracking',
    p_field_name := 'modifiedts',
    p_where := '1=1'
);
```

---

### fw.f_update_load_info
Updates a specified field in the `load_info` table.

**Parameters:**
- `p_load_id`: Load ID (required).
- `p_field_name`: Name of the field in the `load_id` table (required).
- `p_value`: Value to set (required).

**Example Call:**
```sql
SELECT fw.f_update_load_info(
    p_load_id := 1495,
    p_field_name := 'row_cnt',
    p_value := '2135'::text
);
```

---

### fw.f_get_load_function
Retrieves the load function of an object from `fw.objects`.

**Parameters:**
- `p_object_id`: ID of the object (required).

**Example Call:**
```sql
SELECT fw.f_get_load_function(p_object_id := 47);
```

---

### fw.f_insert_table_sql
Loads data from an SQL query into a table.

**Parameters:**
- `p_table_to`: Name of the target table with schema (required).
- `p_sql`: SQL query for inserting data (required).
- `p_truncate_tgt`: Flag to truncate the target table before insertion (optional).

**Example Call:**
```sql
SELECT fw.f_insert_table_sql(
    p_table_to := 'stg_hybris.load_customertracking',
    p_sql := 'select * from stg_hybris.delta_customertracking',
    p_truncate_tgt := true
);
```

---

### fw.f_upsert_table_sql
Updates data in a table using the UPSERT method based on an SQL query.

**Parameters:**
- `p_table_to_name`: Name of the target table with schema (required).
- `p_sql`: SQL query for updated data (required).
- `p_load_id`: Load ID (required).
- `p_delete_duplicates`: Flag to delete duplicates based on the merge key (optional).

**Example Call:**
```sql
SELECT fw.f_upsert_table_sql(
    p_table_to_name := 'stg_hybris.load_customertracking',
    p_sql := 'select * from stg_hybris.delta_customertracking',
    p_load_id := 1424,
    p_delete_duplicates := false
);
```

---

### fw.f_set_session_param
Sets the value of a session parameter.

**Parameters:**
- `p_param_name`: Name of the parameter (required).
- `p_param_value`: Value of the parameter (required).

**Example Call:**
```sql
SELECT fw.f_set_session_param(
    p_param_name := 'fw.load_id',
    p_param_value := '1424'::text
);
```

---

### fw.f_get_session_param
Retrieves the value of a session parameter.

**Parameters:**
- `p_param_name`: Name of the parameter (required).

**Example Call:**
```sql
SELECT fw.f_get_session_param(p_param_name := 'fw.load_id');
```

---

### fw.f_load_object
Loads an object using its `load_function`.

**Parameters:**
- `p_load_id`: Load ID (required).

**Example Call:**
```sql
SELECT fw.f_load_object(p_load_id := 1520);
```

---

### fw.f_load_simple_full
Performs a full reload of the target object with backup table creation.

**Parameters:**
- `p_load_id`: Load ID (required).
- `p_src_table`: Source table or view (required).
- `p_trg_table`: Target table (optional).

**Example Call:**
```sql
SELECT fw.f_load_simple_full(
    p_load_id := 2186,
    p_src_table := 'stg_hybris.load_carts'
);
```

---

### fw.f_load_simple_upsert
Performs an update-insert load of the target object using a merge key.

**Parameters:**
- `p_load_id`: Load ID (required).
- `p_src_table`: Source table or view (required).
- `p_trg_table`: Target table (optional).
- `p_delete_duplicates`: Flag to delete duplicates based on the merge key (optional).

**Example Call:**
```sql
SELECT fw.f_load_simple_upsert(
    p_load_id := 2183,
    p_src_table := 'stg_hybris.load_b2bcompany'
);
```

---

### fw.f_load_simple
Loads the target object without transforming fields using the specified method.

**Parameters:**
- `p_load_id`: Load ID (required).
- `p_src_table`: Source table or view (required).
- `p_trg_table`: Target table (optional).
- `p_delete_duplicates`: Flag to delete duplicates based on the merge key (optional).

**Example Call:**
```sql
SELECT fw.f_load_simple(
    p_load_id := 2183,
    p_src_table := 'stg_hybris.load_b2bcompany'
);
```

---

### fw.f_update_table_sql(text, text, _text, int8)
Updates fields in a specified table based on a set of fields from an SQL query.

**Parameters:**
- `p_table_to_name`: Target table (required).
- `p_sql`: SQL query forming the dataset for updates (required).
- `p_column_list`: List of columns to update (required).
- `p_load_id`: Load ID for determining the merge key (required).

**Example Call:**
```sql
SELECT fw.f_update_table_sql(
    p_table_to_name := 'test_fw.update_table',
    p_sql := 'select 1 as f1,2 as f2,3 as f3',
    p_column_list := array['f1','f2','f3'],
    p_load_id := 999
);
```

---

### fw.f_update_table_sql(text, text, _text, _text)
Updates fields in a specified table based on a set of fields from an SQL query.

**Parameters:**
- `p_table_to_name`: Target table (required).
- `p_sql`: SQL query forming the dataset for updates (required).
- `p_column_list`: List of columns to update (required).
- `p_merge_key`: List of fields for merging (required).

**Example Call:**
```sql
SELECT fw.f_update_table_sql(
    p_table_to_name := 'test_fw.update_table',
    p_sql := 'select 1 as f1,2 as f2,3 as f3',
    p_column_list := array['f1','f2','f3'],
    p_merge_key := array['f1']
);
```

---

### fw.f_get_constant
Retrieves a constant from the `fw.load_constants` table.

**Parameters:**
- `p_constant_name`: Name of the constant (required).

**Example Call:**
```sql
SELECT fw.f_get_constant(p_constant_name := 'c_ext_table_prefix');
```

---

### fw.f_get_where_cond
Retrieves the complete WHERE condition for loading data from an external system for a load ID.

**Parameters:**
- `p_load_id`: Load ID (required).
- `p_table_alias`: Alias of the target table in the WHERE condition (optional).

**Example Call:**
```sql
SELECT fw.f_get_where_cond(
    p_load_id := 1520,
    p_table_alias := 't1'
);
```

---

### fw.f_get_delta_table_name
Retrieves the name of the delta table for a specified load ID.

**Parameters:**
- `p_load_id`: Load ID (required).

**Example Call:**
```sql
SELECT fw.f_get_delta_table_name(p_load_id := 1520);
```

---

### fw.f_get_ext_table_name
Retrieves the name of the external table for a specified load ID.

**Parameters:**
- `p_load_id`: Load ID (required).

**Example Call:**
```sql
SELECT fw.f_get_ext_table_name(p_load_id := 1520);
```

---

### fw.f_get_extr_expression
Retrieves the SQL expression for extracting data from a source table to a target table with transformation rules.

**Parameters:**
- `p_load_id`: Load ID (required).
- `p_source_table`: Source table (required).
- `p_trg_table`: Target table (optional).

**Example Call:**
```sql
SELECT fw.f_get_extr_expression(
    p_load_id := 25202,
    p_source_table := 'stg_hybris.ext_glabels',
    p_trg_table := 'stg_hybris.delta_glabels'
);
```

---

### fw.f_get_extract_where_cond
Retrieves the WHERE condition for extracting data from a source table to a target table with transformation rules.

**Parameters:**
- `p_load_id`: Load ID (required).
- `p_table_alias`: Alias of the source table (optional).

**Example Call:**
```sql
SELECT fw.f_get_extract_where_cond(p_load_id := 25202);
```

---

### fw.f_extract_data
Loads data from an external table for a specified load ID into a staging table.

**Parameters:**
- `p_load_id`: Load ID (required).

**Example Call:**
```sql
SELECT fw.f_extract_data(p_load_id := 1520);
```

---

### fw.f_load_data
Loads data from a staging table for a specified load ID into the target table.

**Parameters:**
- `p_load_id`: Load ID (required).

**Example Call:**
```sql
SELECT fw.f_load_data(p_load_id := 1520);
```

---

### fw.f_delete_table_sql
Deletes data in the target table from a dataset obtained from an SQL query based on the specified merge key.

**Parameters:**
- `p_table_name`: Name of the target table with schema (required).
- `p_sql`: SQL query defining the dataset for deletion from the target table (required).
- `p_merge_key`: List of fields for deletion from the target table (required).

**Example Call:**
```sql
SELECT fw.f_delete_table_sql(
    p_table_name := 'hybris.visits',
    p_sql := 'select 1 as pk',
    p_merge_key := array['pk']
);
```

---

### fw.f_get_pxf_partition
Forms a partitioning string for PXF based on the data load interval.

**Parameters:**
- `p_load_id`: Load ID (required).

**Example Call:**
```sql
SELECT fw.f_get_pxf_partition(p_load_id := 1520);
```

---

### fw.f_create_obj_sql
Creates a table, temporary table, or view based on an SQL query.

**Parameters:**
- `p_schema_name`: Schema of the created object (required).
- `p_obj_name`: Name of the object (required).
- `p_sql_text`: SQL for creating the object (required).
- Additional optional parameters include `p_grants_template`, `p_mat_flg`, `p_storage_opt`, `p_analyze_flg`, `p_temporary`, `p_distr_cls`.

**Example Call:**
```sql
SELECT fw.f_create_obj_sql(
    p_schema_name := 'public',
    p_obj_name := 'new_table',
    p_sql_text := 'SELECT * FROM some_table'
);
```

---

### fw.f_load_delta_update_partitions
Updates data in the target table from the source table methodically using DELTA_UPDATE_PARTITION.

**Parameters:**
- `p_table_from_name`: Name of the source table with schema (required).
- `p_table_to_name`: Name of the target table with schema (required).
- `p_load_id`: Load ID (required).

**Example Call:**
```sql
SELECT fw.f_load_delta_update_partitions(
    p_table_from_name := 'stg_hybris.delta_orders2',
    p_table_to_name := 'src_hybris.orders2',
    p_load_id := 15075
);
```
