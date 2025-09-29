# Custom Data Format Driver

## Description

The **Custom Data Format Driver** allows dynamic loading and execution of user-defined logic to read from or write to files in custom data formats. This design enables users to integrate their own data format parsers without modifying the core framework.

The driver loads a Python module at runtime based on the provided `file_name`, imports the corresponding class (named using PascalCase convention), and invokes its `read_data` or `write_data` method as needed.

## How to Use in YAML Config

To use the **Custom Data Format Driver**, specify `type: custom` under the `data_format` section in the YAML configuration. You also need to provide the `file_name` of the module implementing your custom logic.

### Example YAML Configuration

```yaml
mappings:
  - mapping:
      from:
        store:
          type: local
          path: input/
        data_format:
          type: custom
          file_name: <module name>
          args:
            <key>:<value>
        
```

## Driver File Naming Convention

* File should be placed under the `custom_data_format_drivers` directory inside the project folder.
* File name should match the module name provided in `file_name`, e.g., `python_xml_driver.py`.
* The main class inside the file should follow PascalCase of the file name, e.g., `PythonXmlDriver`.


## Function Arguments

| Argument    | Type   | Mandatory | Description                                                    |
| ----------- | ------ | --------- | -------------------------------------------------------------- |
| `file_path` | `str`  | Yes       | Path to the input or output file                               |
| `config`    | `dict` | Yes       | Configuration dictionary (full mapping context)                |
| `entity`    | `str`  | Yes       | Name of the entity being processed                             |
| `args`      | `dict` | No        | Additional arguments specified in the YAML under `args`        |
| `**kwargs`  | `dict` | No        | Any additional runtime arguments passed by the pipeline engine |


This documentation explains how to configure and implement the **Custom Data Format Driver** to extend the data ingestion and export capabilities using Python modules defined per project.