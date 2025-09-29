# Schema Evolution Configuration Guide

## Overview

This document explains how to configure schema evolution using the `JdbcSchemaEvolution` class. Schema evolution helps keep your target database schema in sync with your source data (e.g., a DataFrame) by adding, dropping, or ignoring columns and adjusting data types.

---

## YAML Configuration Parameters

```yaml
schema_evolution: True
schema_evolution_type: sync     # Options: add, sync, ignore
data_type_sync: source          # Options: source, target, ignore
```

---

## Schema Evolution Configuration Options

| Argument                | Type     | Mandatory | Default Value | Description                                                                 |
|-------------------------|----------|------------|----------------|-----------------------------------------------------------------------------|
| `schema_evolution`      | Boolean  | Yes        | `False`            | Enables schema evolution features such as column add/drop and type sync.   |
| `schema_evolution_type` | String   | Yes        | `ignore`          | Controls column-level schema evolution: `add`, `sync`, or `ignore`.        |
| `data_type_sync`        | String   | No         | `ignore`       | Determines how data type changes are handled: `source`, `target`, `ignore`.|

---

## Supported `schema_evolution_type` Values

### `add`
- Adds new columns from the source that do not exist in the target.
- No existing columns are removed or modified.

Example:
```sql
ALTER TABLE `orders` ADD COLUMN `discount` DOUBLE;
```

---

### `sync`
- Adds new columns and drops columns that are not present in the source schema.
- Keeps source and target schemas in full alignment.

Example:
```sql
ALTER TABLE `orders` DROP COLUMN `legacy_code`;
ALTER TABLE `orders` ADD COLUMN `shipping_cost` DOUBLE;
```

---

### `ignore`
- No schema changes will be made.
- Table columns will remain untouched.
- Use this when schema evolution is not desired.

---

## Data Type Synchronization (`data_type_sync`)

| Value     | Behavior                                                                                   |
|-----------|--------------------------------------------------------------------------------------------|
| `source`  | Alters target table column types to match the source schema. (`ALTER COLUMN`)             |
| `target`  | Converts DataFrame types to match the target DB schema. (`df.astype()`)                   |
| `ignore`  | Skips all data type changes. No type adjustments are performed.                           |

---

## Supported Databases for Type Mapping

| Database     | Column Add | Column Drop | Type Change | Notes                     |
|--------------|------------|-------------|-------------|---------------------------|
| MySQL        | Yes        | Yes         | Yes         | Full support              |
| PostgreSQL   | Yes        | Yes         | Yes         | Full support              |
| SQLite       | Yes        | Yes         | No          | Type change not supported |
| Oracle       | Yes        | Yes         | Yes         | Supported                 |
| Snowflake    | Yes        | Yes         | Yes         | Supported                 |

---

## Notes

- `schema_evolution: True` must be enabled for any schema changes to occur.
- Use `schema_evolution_type: ignore` to completely bypass any column-level modifications.
- Use `data_type_sync: ignore` to bypass type changes while still allowing column add/drop (if enabled).
