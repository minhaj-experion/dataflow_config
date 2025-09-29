
## Description

The `PythonCassandraDriver` class enables efficient and scalable interaction with Apache Cassandra for reading and writing operations within a configuration-driven ETL pipeline. It supports various read modes such as `full_load` and `incremental`, and supports write modes including `overwrite`, `append`, `upsert`, `upsert_only`, and `append_delete`.

Data is read from Cassandra tables with support for column selection, offset-based incremental reads using timestamp columns, and customizable CQL filters. Writing to the destination supports multiple modes, including append, append_delete, overwrite, upsert, and upsert_only.

This driver is ideal for integrating Cassandra into real-time and batch ETL workflows with support for schema-driven execution, soft delete flags, and offset tracking for incremental operations.

---

## How to Use in YAML Config

YAML configuration for the `PythonCassandraDriver` enables seamless interaction with Cassandra keyspaces and tables. It supports both `full_load` and `incremental` read modes.

The driver supports `upsert`, `upsert_only`,`overwrite`,`append` and `append_delete` write operations, as well as `soft delete` and `hard delete` .

### YAML Example

```yaml 
  mappings:
    - mapping:
        mapping_name: cassandra_sample      # Unique mapping identifier
        write_keys: True                    # Enable key tracking
        first_load: False
        load_type: incremental              # Perform incremental read
        write_mode: upsert                  # Use Delta merge for insert/update
        delete_type: keys                   # Use key comparison for delete detection
        delete_mode: soft_delete            # Logically delete records (hard_delete also supported)
  
        from:
          store:
            type: cassandra
            db_name: test_keyspace                  # Target Cassandra keyspace
            connection_url: localhost               # Cassandra host
            port: 9042                              # Cassandra port
            user: cassandra                         # Cassandra username
            password: cassandra                     # Cassandra password                            
          entity:
            include:
              - users                       # Entities to process
            columns_to_read:                # Read only specified columns
              - id
              - name
              - age
              - city
            filters:                        # Filter condition before processing
              - column: age
                operator: ">"
                value: 30
            primary_keys:
              - id                          # Required for upsert/merge
          offset:
            joining_condition: OR           # Join offset columns with OR
            offset_columns:
              - create_dttm
              - upd_dttm
  
        to:
          store:
            type: cassandra
            db_name: test_keyspace                  # Target Cassandra keyspace
            connection_url: localhost               # Cassandra host
            port: 9042                              # Cassandra port
            user: cassandra                         # Cassandra username
            password: cassandra                     # Cassandra password                            
          entity:
            name: output_{${entity}$}               # Target Cassandra table name
            primary_key:
              - id                                  # Primary key used for insert/update/delete
            delete_log_columns:                     # Mark soft deletes using a column
              - column: DELETED
                value: 1
            update_log_columns:                     # Mark updated records using a column
              - column: UPDATED
                value: 1

```

---

## Read Configuration

| Argument                    | Type   | Mandatory         | Description                                                                                     |
|-----------------------------|--------|-------------------|-------------------------------------------------------------------------------------------------|
| `store.type`                | str    | Yes               | Must be `cassandra`.                                                                            |
| `store.hosts`               | list   | Yes               | List of Cassandra hosts.                                                                        |
| `store.port`                | int    | No                | Cassandra native port (default: 9042).                                                           |
| `store.keyspace`            | str    | Yes               | Keyspace to query from.                                                                         |
| `entity.include`            | list   | Yes               | List of tables/entities to include.                                                             |
| `entity.columns_to_read`    | list   | No                | List of columns to project in the query.                                                        |
| `entity.filters`            | list   | No                | Optional row-level filters based on CQL WHERE clause.                                           |
| `entity.primary_keys`       | list   | Yes               | Primary key(s) for identifying records.                                                         |
| `offset.offset_columns`     | list   | No                | Columns used to track changes for incremental loads. Usually `updated_at` or `write_time`.      |
| `offset.joining_condition`  | str    | No                | How to combine offset filters (`AND`, `OR`).                                                    |

---

## Write Configuration

| Argument                    | Type   | Mandatory        | Description                                                                                     |
|-----------------------------|--------|------------------|-------------------------------------------------------------------------------------------------|
| `store.type`                | str    | Yes              | Must be `cassandra`.                                                                            |
| `store.hosts`               | list   | Yes              | List of Cassandra hosts.                                                                        |
| `store.keyspace`            | str    | Yes              | Target keyspace.                                                                                |
| `entity.name`               | str    | Yes              | Target table name.                                                                              |
| `entity.primary_key`        | list   | Yes              | List of primary key columns for insert/update/delete.                                           |
| `write_mode`                | str    | Yes              | `overwrite`, `upsert`, `upsert_only`or`append`,`append_delste`.                                                                |
| `entity.delete_log_columns` | list   | No               | Columns and values used to logically mark records as deleted.                                   |
| `entity.update_log_columns` | list   | No               | Columns and values used to mark updated records.                                                |

---

## Supported Load & Write Strategies

### Load Types

| `load_type`   | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| `full_load`   | Loads the entire dataset from Cassandra.                                    |
| `incremental` | Uses `offset_columns` (e.g., `updated_at`) to only load new/modified rows.  |

### Write Modes

| `write_mode`     | Description                                                                                           |
|------------------|-------------------------------------------------------------------------------------------------------|
| `overwrite`      | Replaces the entire target table by first truncating and then inserting new data.                     |
| `append`         | Adds new records to the target table without modifying existing rows.                                 |
| `upsert`         | Inserts new records and updates existing ones using Cassandra’s primary key-based insert behavior.    |
| `upsert_only`    | Only inserts or updates matching records; does not handle deletion of missing keys.                   |
| `append_delete`  | Appends new/updated data and deletes records from the target table based on primary key comparison.   |


### Delete Modes

| `delete_mode`   | Description                                                            |
|-----------------|------------------------------------------------------------------------|
| `soft_delete`   | Marks a row as deleted using a column like `is_deleted = true`.       |
| `hard_delete`   | Removes the record from Cassandra using a DELETE statement.            |

---

## Functionality

### Ingestion Flow

- **Read from Cassandra Table**  
  The driver fetches records from specified tables using projected columns and filter conditions.

- **Apply Offsets for Incremental Load**  
  Timestamp-based or offset column filters are applied to load only new/updated rows.

**Write to Cassandra Table**  
Data is written to the destination keyspace/table based on the configured `write_mode`:

- `overwrite`: replaces all existing records in the target table.
- `append`: inserts only new records without modifying existing ones.
- `upsert`: inserts new records and updates existing ones based on primary key.
- `upsert_only`: updates matching records and inserts new ones; does not delete missing data.
- `append_delete`: inserts/updates records and deletes removed ones based on primary key comparison.


- **Soft Delete Support**  
  Records can be logically deleted by setting `is_deleted = true`.

- **Offset Table Update**  
  Latest timestamp offsets are recorded for each table post-ingestion.