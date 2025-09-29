#!/usr/bin/env python3
"""
DataFlow xLerate Web UI
A web interface for creating YAML configuration files
"""

from flask import Flask, render_template, request, jsonify, send_file
import yaml
import os
import json
from pathlib import Path
from dataflow_xlerate.config.validator import ConfigValidator
from dataflow_xlerate.config.parser import ConfigParser

app = Flask(__name__, template_folder='templates', static_folder='static')

# Store configuration data
CONFIG_DATA = {
    'globals': {},
    'pipeline': {},
    'mappings': []
}

@app.route('/')
def index():
    """Main configuration page"""
    return render_template('index.html')

@app.route('/api/ui-registry')
def get_ui_registry():
    """Get complete UI registry with store types, data formats, and transformations"""
    return jsonify({
        'stores': {
            'local': {
                'label': 'Local File System',
                'description': 'Read/write files on local storage',
                'icon': 'fas fa-folder',
                'fields': [
                    {
                        'key': 'path',
                        'label': 'Path',
                        'type': 'text',
                        'path': '<ctx>.store.path',
                        'required': True,
                        'placeholder': './data/input/',
                        'help': 'Directory path for files'
                    }
                ]
            },
            'jdbc': {
                'label': 'JDBC Database',
                'description': 'Connect to SQL databases (PostgreSQL, MySQL, etc.)',
                'icon': 'fas fa-database',
                'fields': [
                    {
                        'key': 'db_name',
                        'label': 'Database Name',
                        'type': 'text',
                        'path': '<ctx>.store.db_name',
                        'required': True,
                        'placeholder': '${PGDATABASE}'
                    },
                    {
                        'key': 'host',
                        'label': 'Host',
                        'type': 'text',
                        'path': '<ctx>.store.host',
                        'required': False,
                        'placeholder': 'localhost',
                        'default': 'localhost'
                    },
                    {
                        'key': 'port',
                        'label': 'Port',
                        'type': 'number',
                        'path': '<ctx>.store.port',
                        'required': False,
                        'placeholder': '5432'
                    },
                    {
                        'key': 'username',
                        'label': 'Username',
                        'type': 'text',
                        'path': '<ctx>.store.username',
                        'required': False
                    },
                    {
                        'key': 'password',
                        'label': 'Password',
                        'type': 'secret',
                        'path': '<ctx>.store.password',
                        'required': False
                    },
                    {
                        'key': 'db_type',
                        'label': 'Database Type',
                        'type': 'select',
                        'path': '<ctx>.store.db_type',
                        'required': False,
                        'options': [
                            {'value': 'postgresql', 'label': 'PostgreSQL'},
                            {'value': 'mysql', 'label': 'MySQL'},
                            {'value': 'sqlite', 'label': 'SQLite'}
                        ],
                        'default': 'postgresql'
                    }
                ]
            },
            'api': {
                'label': 'API / REST',
                'description': 'Connect to REST APIs and web services',
                'icon': 'fas fa-cloud',
                'fields': [
                    {
                        'key': 'url',
                        'label': 'API URL',
                        'type': 'text',
                        'path': '<ctx>.store.url',
                        'required': True,
                        'placeholder': 'https://api.example.com/data'
                    },
                    {
                        'key': 'authentication_type',
                        'label': 'Authentication Type',
                        'type': 'select',
                        'path': '<ctx>.store.authentication_type',
                        'required': True,
                        'options': [
                            {'value': 'none', 'label': 'None'},
                            {'value': 'bearer', 'label': 'Bearer Token'},
                            {'value': 'basic', 'label': 'Basic Auth'},
                            {'value': 'apikey', 'label': 'API Key'},
                            {'value': 'oauth2', 'label': 'OAuth2'}
                        ],
                        'default': 'none'
                    }
                ]
            }
        },
        'formats': {
            'csv': {
                'label': 'CSV (Comma-Separated Values)',
                'description': 'Text files with comma-separated data',
                'icon': 'fas fa-file-csv',
                'fields': [
                    {
                        'key': 'delimiter',
                        'label': 'Delimiter',
                        'type': 'select',
                        'path': '<ctx>.data_format.delimiter',
                        'required': False,
                        'options': [
                            {'value': ',', 'label': 'Comma (,)'},
                            {'value': ';', 'label': 'Semicolon (;)'},
                            {'value': '\\t', 'label': 'Tab'},
                            {'value': '|', 'label': 'Pipe (|)'}
                        ],
                        'default': ',',
                        'show_if': {'<ctx>.data_format.type': 'csv'}
                    },
                    {
                        'key': 'encoding',
                        'label': 'Encoding',
                        'type': 'select',
                        'path': '<ctx>.data_format.encoding',
                        'required': False,
                        'options': [
                            {'value': 'utf-8', 'label': 'UTF-8'},
                            {'value': 'ascii', 'label': 'ASCII'},
                            {'value': 'latin-1', 'label': 'Latin-1'}
                        ],
                        'default': 'utf-8',
                        'show_if': {'<ctx>.data_format.type': 'csv'}
                    },
                    {
                        'key': 'quotechar',
                        'label': 'Quote Character',
                        'type': 'text',
                        'path': '<ctx>.data_format.quotechar',
                        'required': False,
                        'placeholder': '"',
                        'default': '"',
                        'show_if': {'<ctx>.data_format.type': 'csv'}
                    },
                    {
                        'key': 'header',
                        'label': 'Header Row',
                        'type': 'select',
                        'path': '<ctx>.data_format.header',
                        'required': False,
                        'options': [
                            {'value': 0, 'label': 'First row (0)'},
                            {'value': None, 'label': 'No header'}
                        ],
                        'default': 0,
                        'show_if': {'<ctx>.data_format.type': 'csv'}
                    }
                ]
            },
            'parquet': {
                'label': 'Parquet (Columnar Format)',
                'description': 'Apache Parquet columnar storage format',
                'icon': 'fas fa-file-alt',
                'fields': [
                    {
                        'key': 'engine',
                        'label': 'Engine',
                        'type': 'select',
                        'path': '<ctx>.data_format.engine',
                        'required': False,
                        'options': [
                            {'value': 'pyarrow', 'label': 'PyArrow'},
                            {'value': 'fastparquet', 'label': 'FastParquet'}
                        ],
                        'default': 'pyarrow',
                        'show_if': {'<ctx>.data_format.type': 'parquet'}
                    },
                    {
                        'key': 'compression',
                        'label': 'Compression',
                        'type': 'select',
                        'path': '<ctx>.data_format.compression',
                        'required': False,
                        'options': [
                            {'value': 'snappy', 'label': 'Snappy'},
                            {'value': 'gzip', 'label': 'GZip'},
                            {'value': 'brotli', 'label': 'Brotli'},
                            {'value': 'none', 'label': 'None'}
                        ],
                        'default': 'snappy',
                        'show_if': {'<ctx>.data_format.type': 'parquet'}
                    }
                ]
            },
            'json': {
                'label': 'JSON (JavaScript Object Notation)',
                'description': 'JavaScript Object Notation format',
                'icon': 'fas fa-file-code',
                'fields': [
                    {
                        'key': 'json_structure',
                        'label': 'JSON Structure',
                        'type': 'select',
                        'path': '<ctx>.data_format.json_structure',
                        'required': False,
                        'options': [
                            {'value': 'records', 'label': 'Records (Array of Objects)'},
                            {'value': 'split', 'label': 'Split (Index/Columns/Data)'},
                            {'value': 'index', 'label': 'Index (Keyed by Index)'},
                            {'value': 'columns', 'label': 'Columns (Keyed by Column)'}
                        ],
                        'default': 'records',
                        'show_if': {'<ctx>.data_format.type': 'json'}
                    },
                    {
                        'key': 'file_encoding',
                        'label': 'Encoding',
                        'type': 'select',
                        'path': '<ctx>.data_format.file_encoding',
                        'required': False,
                        'options': [
                            {'value': 'utf-8', 'label': 'UTF-8'},
                            {'value': 'ascii', 'label': 'ASCII'}
                        ],
                        'default': 'utf-8',
                        'show_if': {'<ctx>.data_format.type': 'json'}
                    }
                ]
            },
            'xml': {
                'label': 'XML (Extensible Markup Language)',
                'description': 'XML structured data format',
                'icon': 'fas fa-file-code',
                'fields': [
                    {
                        'key': 'xpath',
                        'label': 'XPath Expression',
                        'type': 'text',
                        'path': '<ctx>.data_format.xpath',
                        'required': False,
                        'placeholder': './/*',
                        'default': './/*',
                        'show_if': {'<ctx>.data_format.type': 'xml'},
                        'help': 'XPath to locate records in XML'
                    },
                    {
                        'key': 'file_encoding',
                        'label': 'Encoding',
                        'type': 'select',
                        'path': '<ctx>.data_format.file_encoding',
                        'required': False,
                        'options': [
                            {'value': 'utf-8', 'label': 'UTF-8'},
                            {'value': 'ascii', 'label': 'ASCII'}
                        ],
                        'default': 'utf-8',
                        'show_if': {'<ctx>.data_format.type': 'xml'}
                    }
                ]
            },
            'jdbc': {
                'label': 'JDBC (Database Table)',
                'description': 'Direct database table format',
                'icon': 'fas fa-table',
                'fields': []
            }
        },
        'transformations': {
            'cleanup': {
                'label': 'Basic Cleanup',
                'description': 'Remove empty rows, trim whitespace, clean column names',
                'icon': 'fas fa-broom',
                'fields': [
                    {
                        'key': 'remove_empty_rows',
                        'label': 'Remove Empty Rows',
                        'type': 'boolean',
                        'path': 'remove_empty_rows',
                        'default': True
                    },
                    {
                        'key': 'remove_duplicates',
                        'label': 'Remove Duplicates',
                        'type': 'boolean',
                        'path': 'remove_duplicates',
                        'default': False
                    },
                    {
                        'key': 'trim_whitespace',
                        'label': 'Trim Whitespace',
                        'type': 'boolean',
                        'path': 'trim_whitespace',
                        'default': True
                    },
                    {
                        'key': 'clean_column_names',
                        'label': 'Clean Column Names',
                        'type': 'boolean',
                        'path': 'clean_column_names',
                        'default': True
                    }
                ]
            },
            'schema_map': {
                'label': 'Schema Mapping',
                'description': 'Rename columns and select specific fields',
                'icon': 'fas fa-exchange-alt',
                'fields': [
                    {
                        'key': 'column_mapping',
                        'label': 'Column Mapping',
                        'type': 'code',
                        'path': 'column_mapping',
                        'help': 'JSON object mapping old names to new names',
                        'placeholder': '{"old_name": "new_name"}'
                    },
                    {
                        'key': 'selected_columns',
                        'label': 'Selected Columns',
                        'type': 'text',
                        'path': 'selected_columns',
                        'help': 'Comma-separated list of columns to keep',
                        'placeholder': 'id, name, email'
                    }
                ]
            },
            'filter': {
                'label': 'Data Filtering',
                'description': 'Filter rows based on conditions',
                'icon': 'fas fa-filter',
                'fields': [
                    {
                        'key': 'conditions',
                        'label': 'Filter Conditions',
                        'type': 'code',
                        'path': 'conditions',
                        'help': 'Array of filter conditions',
                        'placeholder': '[{"column": "age", "operator": ">", "value": 18}]'
                    }
                ]
            }
        }
    })

@app.route('/api/store-types')
def get_store_types():
    """Get available store types (legacy endpoint)"""
    registry = get_ui_registry().get_json()
    stores = {}
    for key, store_data in registry['stores'].items():
        stores[key] = {
            'name': store_data['label'],
            'description': store_data['description'],
            'fields': [field['key'] for field in store_data['fields']]
        }
    return jsonify(stores)

@app.route('/api/data-formats')
def get_data_formats():
    """Get available data formats (legacy endpoint)"""
    registry = get_ui_registry().get_json()
    formats = {}
    for key, format_data in registry['formats'].items():
        formats[key] = {
            'name': format_data['label'],
            'description': format_data['description']
        }
    return jsonify(formats)

@app.route('/api/transformations')
def get_transformations():
    """Get available transformations (legacy endpoint)"""
    registry = get_ui_registry().get_json()
    transformations = {}
    for key, transform_data in registry['transformations'].items():
        transformations[key] = {
            'name': transform_data['label'],
            'description': transform_data['description'],
            'fields': [field['key'] for field in transform_data['fields']]
        }
    return jsonify(transformations)

@app.route('/api/config', methods=['GET'])
def get_config():
    """Get current configuration"""
    return jsonify(CONFIG_DATA)

@app.route('/api/config', methods=['POST'])
def update_config():
    """Update configuration"""
    global CONFIG_DATA
    data = request.get_json()
    
    if 'globals' in data:
        CONFIG_DATA['globals'] = data['globals']
    if 'pipeline' in data:
        CONFIG_DATA['pipeline'] = data['pipeline']
    if 'mappings' in data:
        CONFIG_DATA['mappings'] = data['mappings']
    
    return jsonify({'status': 'success'})

@app.route('/api/validate', methods=['POST'])
def validate_config():
    """Validate the current configuration"""
    try:
        validator = ConfigValidator()
        result = validator.validate(CONFIG_DATA)
        
        return jsonify({
            'valid': result.is_valid,
            'errors': result.errors,
            'warnings': result.warnings
        })
    except Exception as e:
        return jsonify({
            'valid': False,
            'errors': [str(e)],
            'warnings': []
        })

@app.route('/api/generate-yaml', methods=['POST'])
def generate_yaml():
    """Generate YAML from current configuration"""
    try:
        yaml_content = yaml.dump(CONFIG_DATA, default_flow_style=False, sort_keys=False)
        return jsonify({
            'yaml': yaml_content,
            'status': 'success'
        })
    except Exception as e:
        return jsonify({
            'yaml': '',
            'status': 'error',
            'message': str(e)
        })

@app.route('/api/download-yaml', methods=['POST'])
def download_yaml():
    """Download YAML configuration file"""
    try:
        yaml_content = yaml.dump(CONFIG_DATA, default_flow_style=False, sort_keys=False)
        
        # Create temporary file
        temp_path = Path('/tmp/pipeline_config.yaml')
        with open(temp_path, 'w') as f:
            f.write(yaml_content)
        
        return send_file(
            temp_path,
            as_attachment=True,
            download_name='pipeline_config.yaml',
            mimetype='application/x-yaml'
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/load-yaml', methods=['POST'])
def load_yaml():
    """Load YAML configuration from file"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        content = file.read().decode('utf-8')
        config = yaml.safe_load(content)
        
        global CONFIG_DATA
        CONFIG_DATA = config
        
        return jsonify({
            'status': 'success',
            'config': CONFIG_DATA
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Create templates and static directories
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    
    app.run(host='0.0.0.0', port=5000, debug=True)