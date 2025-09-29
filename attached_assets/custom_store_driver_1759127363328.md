# Custom Store Driver

## Description

The Custom Store Driver lets users add their own logic to read from or write to different storage systems. Its plugin-like design makes it easy to expand storage support without changing the main pipeline system.
The driver dynamically loads a Python module based on the provided `file_name`, instantiates a class (named in PascalCase), and invokes its `pull_data` or `push_data` method.

---

## How to Use in YAML Config

To use the **Custom Store Driver**, specify `type: custom` under the `store` section in the YAML configuration. You must also provide the `file_name` of the module implementing your custom logic.

### Example YAML Configuration

```yaml
mappings:
  - mapping:
      from:
        store:
          type: custom
          file_name: <file_name>
          project_name: <project_name>
          args:
          <key>:<value>
        data_format :
          type : <file_type>  
      to :
        file_name: <file_name>
        project_name: <project_name>     
```

---

## Driver File Naming Convention

- File must be placed under:  
  `projects/<project_name>/custom_store_driver/`
- File name must match the module name provided in `file_name`, e.g., `api_store_driver.py`
- The main class inside the file must follow PascalCase, e.g., `ApiStoreDriver`

---

## Function Requirements

### `pull_data(mapping_config, entity)`

Used to **fetch** data from the custom storage location.

### `push_data(config, entity, df, deletes_df=None)`

Used to **write** data to the custom storage location.

---

## Function Arguments

| Argument      | Type              | Mandatory | Description                                                                 |
|---------------|-------------------|-----------|-----------------------------------------------------------------------------|
| `config`      | `dict`            | Yes       | Extracted config from YAML for this mapping (`args`, `url`, etc.)          |
| `entity`      | `str`             | Yes       | Entity name (e.g., "posts")                                                |
| `df`          | `pandas.DataFrame`| Yes       | Data to be pushed                                                          |
| `deletes_df`  | `pandas.DataFrame`| No        | Records to delete (optional, used for delta or soft delete logic)          |
---

## Notes

- Ensure the class name inside your custom driver file **matches** the PascalCase version of the file name.
- The `args` key in the YAML can be used to pass configuration like API URLs, authentication info, or field mappings.
- Both `pull_data` and `push_data` should return a status or result, even if it's just a success flag.