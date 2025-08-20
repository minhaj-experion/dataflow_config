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

@app.route('/api/store-types')
def get_store_types():
    """Get available store types"""
    return jsonify({
        'jdbc': {
            'name': 'JDBC Database',
            'description': 'Connect to SQL databases (PostgreSQL, MySQL, etc.)',
            'fields': ['db_name', 'host', 'port', 'username', 'password', 'db_type']
        },
        'local': {
            'name': 'Local File System',
            'description': 'Read/write files on local storage',
            'fields': ['path']
        }
    })

@app.route('/api/data-formats')
def get_data_formats():
    """Get available data formats"""
    return jsonify({
        'csv': {'name': 'CSV', 'description': 'Comma-separated values'},
        'parquet': {'name': 'Parquet', 'description': 'Apache Parquet columnar format'},
        'json': {'name': 'JSON', 'description': 'JavaScript Object Notation'},
        'jdbc': {'name': 'JDBC', 'description': 'Database table format'}
    })

@app.route('/api/transformations')
def get_transformations():
    """Get available transformations"""
    return jsonify({
        'cleanup': {
            'name': 'Basic Cleanup',
            'description': 'Remove empty rows, trim whitespace, clean column names',
            'fields': ['remove_empty_rows', 'remove_duplicates', 'trim_whitespace', 'clean_column_names']
        },
        'schema_map': {
            'name': 'Schema Mapping',
            'description': 'Rename columns and select specific fields',
            'fields': ['column_mapping', 'selected_columns']
        },
        'filter': {
            'name': 'Data Filtering',
            'description': 'Filter rows based on conditions',
            'fields': ['conditions']
        }
    })

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