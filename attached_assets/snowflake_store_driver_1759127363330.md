
# Snowflake Store Driver Documentation

The **Snowflake Store Driver** is a configurable connector for reading data from and writing data to Snowflake. It supports features such as incremental loads, schema evolution, write mode control, offset-based filtering, timestamp normalization, and key-based merge operations. It is intended to be integrated into flexible data pipeline workflows.

---

## Configuration Fields Breakdown

| Argument                    | Type       | Mandatory | Default        | Description |
|-----------------------------|------------|-----------|----------------|-------------|
| `first_load`                | `bool`     | No        | `false`        | When `true`, performs a full load ignoring offset conditions. |
| `load_type`                 | `str`      | Yes       | —              | Type of load operation: `incremental` or `full`. |
| `write_mode`                | `str`      | Yes       | —              | One of: `overwrite`, `upsert`, `append`, `append_delete`, `upsert_only`. |
| `write_keys`                | `bool`     | No        | `false`        | Whether to extract only the keys from the source for comparison. |
| `schema_evolution`          | `bool`     | No        | `false`        | Enables automatic schema comparison and alignment. |
| `schema_evolution_type`     | `str`      | No        | `ignore`       | Defines schema change behavior: `ignore`, `add`, `sync`. |
| `data_type_sync`            | `str`      | No        | `ignore`       | Type alignment strategy: `ignore`, `source`, `target`. |
| `delete_mode`               | `str`      | No        | `hard_delete`  | Controls how unmatched data is deleted: `hard_delete` or `soft_delete`. |

---

## Offset Configuration

The `offset` section enables incremental loads by filtering based on timestamp or numeric columns.

```yaml
from:
  offset:
    joining_condition: OR
    offset_columns:
      - created
      - updated
```

- `joining_condition`: Logical operator between offset conditions (`AND` or `OR`).
- `offset_columns`: List of columns used for filtering incremental changes.

---

## From Section (Source Configuration)

### from.store

| Field               | Type     | Mandatory | Description |
|---------------------|----------|-----------|-------------|
| `user`              | `str`    | Yes       | Snowflake user name. |
| `password`          | `str`    | Yes       | Snowflake user password. |
| `account`           | `str`    | Yes       | Snowflake account identifier. |
| `warehouse`         | `str`    | Yes       | Snowflake virtual warehouse. |
| `database`          | `str`    | Yes       | Database name in Snowflake. |
| `schema_name`       | `str`    | Yes       | Schema name containing the source table or view. |
| `time_stamp_columns`| `list`   | No        | Timestamp fields to be parsed and normalized. |

### from.entity

| Field             | Type       | Mandatory | Description |
|-------------------|------------|-----------|-------------|
| `name`            | `str`      | Yes       | Name of the source table or view. |
| `primary_keys`    | `list`     | Yes       | List of primary key fields used for joins and deduplication. |
| `columns_to_read` | `list`     | No        | Columns to select (ignored if `custom_query` is used). |
| `custom_query`    | `str`      | No        | Optional raw SQL query to override default select logic. |
| `filter_condition`| `str`      | No        | WHERE clause for filtering source data. |

---

## To Section (Target Configuration)

### to.store

| Field               | Type     | Mandatory | Description |
|---------------------|----------|-----------|-------------|
| `user`              | `str`    | Yes       | Target Snowflake user name. |
| `password`          | `str`    | Yes       | Target Snowflake password. |
| `account`           | `str`    | Yes       | Target Snowflake account identifier. |
| `warehouse`         | `str`    | Yes       | Target virtual warehouse. |
| `database`          | `str`    | Yes       | Target database. |
| `schema_name`       | `str`    | Yes       | Target schema. |
| `chunk_size`        | `int`    | No        | Number of rows to write per batch. Default is `10000`. |
| `timestamp_columns` | `list`   | No        | Timestamp columns to normalize. |

### to.entity

| Field                | Type         | Mandatory | Description |
|----------------------|--------------|-----------|-------------|
| `name`               | `str`        | Yes       | Target table name. |
| `primary_keys`       | `list`       | Yes       | List of primary key fields. |
| `delete_log_columns` | `list[dict]` | No        | Columns used to track soft-deletes (e.g., `deleted_flag`). |
| `update_log_columns` | `list[dict]` | No        | Columns used for tracking updates (e.g., `updated_by`). |

---

## Supported Write Modes

| Mode           | Description |
|----------------|-------------|
| `overwrite`    | Replaces all data in the target table by truncating and rewriting. |
| `append`       | Adds new rows without deleting or updating any existing data. |
| `upsert`       | Updates matching rows, inserts new rows, and optionally deletes unmatched rows depending on `delete_mode`. |
| `append_delete`| Inserts new rows and deletes records not present in the source. |
| `upsert_only`  | Performs only updates and inserts; deletions are skipped even if unmatched data exists. |

---

## Supported Delete Modes

| Mode         | Description |
|--------------|-------------|
| `hard_delete`| Physically removes rows from the target table. *(default)* |
| `soft_delete`| Updates the rows with specific flags/columns instead of removing rows. |

> Note: `delete_mode` applies to `upsert`, `append_delete` where applicable.

---

## Example Configuration

```yaml
first_load: false
load_type: incremental
write_mode: upsert
write_keys: true

schema_evolution: true
schema_evolution_type: sync
data_type_sync: target
delete_mode: soft_delete

from:
  offset:
    joining_condition: OR
    offset_columns:
      - created
      - updated
  store:
    user: USER
    password: PASSWORD
    account: ACCOUNT
    warehouse: WH
    database: SOURCE_DB
    schema_name: SRC_SCHEMA
    time_stamp_columns:
      - created
      - updated
  entity:
    name: source_table
    primary_keys:
      - id
    columns_to_read:
      - id
      - name
      - created
      - updated
    custom_query: ""
    filter_condition: "status = 'active'"

to:
  store:
    user: USER
    password: PASSWORD
    account: ACCOUNT
    warehouse: WH
    database: TARGET_DB
    schema_name: TGT_SCHEMA
    chunk_size: 10000
    timestamp_columns:
      - created
      - updated
  entity:
    name: target_table
    primary_keys:
      - id
    delete_log_columns:
      - column: deleted_flag
        value: true
    update_log_columns:
      - column: updated_by
        value: "system"
```
