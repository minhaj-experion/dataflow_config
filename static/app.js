// DataFlow xLerate Configuration Builder JavaScript

let storeTypes = {};
let dataFormats = {};
let transformations = {};
let currentConfig = {
    globals: {},
    pipeline: {},
    mappings: []
};

// Initialize the application
document.addEventListener('DOMContentLoaded', function() {
    loadMetadata();
    bindEvents();
    updatePreview();
});

// Load metadata (store types, formats, transformations)
async function loadMetadata() {
    try {
        const [storeResponse, formatResponse, transformResponse] = await Promise.all([
            fetch('/api/store-types'),
            fetch('/api/data-formats'),
            fetch('/api/transformations')
        ]);
        
        storeTypes = await storeResponse.json();
        dataFormats = await formatResponse.json();
        transformations = await transformResponse.json();
        
        console.log('Metadata loaded successfully');
    } catch (error) {
        console.error('Error loading metadata:', error);
    }
}

// Bind form events
function bindEvents() {
    // Global settings
    document.getElementById('global-env').addEventListener('change', updateConfig);
    document.getElementById('global-log-level').addEventListener('change', updateConfig);
    
    // Pipeline settings
    document.getElementById('pipeline-name').addEventListener('input', updateConfig);
    document.getElementById('pipeline-platform').addEventListener('change', updateConfig);
    document.getElementById('pipeline-engine').addEventListener('change', updateConfig);
    document.getElementById('pipeline-retry-mode').addEventListener('change', updateConfig);
}

// Update configuration from form inputs
function updateConfig() {
    // Update globals
    currentConfig.globals = {
        env: document.getElementById('global-env').value,
        log_level: document.getElementById('global-log-level').value
    };
    
    // Update pipeline
    currentConfig.pipeline = {
        pipeline_name: document.getElementById('pipeline-name').value,
        platform: {
            type: document.getElementById('pipeline-platform').value
        },
        engine: {
            type: document.getElementById('pipeline-engine').value
        },
        retry_mode: document.getElementById('pipeline-retry-mode').value
    };
    
    updatePreview();
}

// Add new mapping
function addMapping() {
    const mappingId = Date.now();
    const mapping = {
        mapping: {
            mapping_name: `mapping_${currentConfig.mappings.length + 1}`,
            load_type: 'full',
            write_mode: 'overwrite',
            from: {
                store: { type: 'jdbc' },
                data_format: { type: 'jdbc' },
                entity: { include: [] }
            },
            to: {
                store: { type: 'local' },
                data_format: { type: 'csv' }
            },
            transformations: []
        }
    };
    
    currentConfig.mappings.push(mapping);
    renderMapping(mapping, mappingId, currentConfig.mappings.length - 1);
    updatePreview();
}

// Render mapping UI
function renderMapping(mapping, mappingId, index) {
    const container = document.getElementById('mappings-container');
    const mappingDiv = document.createElement('div');
    mappingDiv.className = 'mapping-card';
    mappingDiv.id = `mapping-${mappingId}`;
    
    mappingDiv.innerHTML = `
        <div class="d-flex justify-content-between align-items-center mb-3">
            <h6><i class="fas fa-exchange-alt"></i> Mapping ${index + 1}</h6>
            <button class="btn btn-sm btn-outline-danger" onclick="removeMapping(${mappingId}, ${index})">
                <i class="fas fa-trash"></i>
            </button>
        </div>
        
        <!-- Basic Settings -->
        <div class="row mb-3">
            <div class="col-md-4">
                <label class="form-label">Mapping Name</label>
                <input type="text" class="form-control" value="${mapping.mapping.mapping_name}" 
                       onchange="updateMappingField(${index}, 'mapping_name', this.value)">
            </div>
            <div class="col-md-4">
                <label class="form-label">Load Type</label>
                <select class="form-select" onchange="updateMappingField(${index}, 'load_type', this.value)">
                    <option value="full" ${mapping.mapping.load_type === 'full' ? 'selected' : ''}>Full Load</option>
                    <option value="incremental" ${mapping.mapping.load_type === 'incremental' ? 'selected' : ''}>Incremental</option>
                </select>
            </div>
            <div class="col-md-4">
                <label class="form-label">Write Mode</label>
                <select class="form-select" onchange="updateMappingField(${index}, 'write_mode', this.value)">
                    <option value="overwrite" ${mapping.mapping.write_mode === 'overwrite' ? 'selected' : ''}>Overwrite</option>
                    <option value="append" ${mapping.mapping.write_mode === 'append' ? 'selected' : ''}>Append</option>
                    <option value="upsert" ${mapping.mapping.write_mode === 'upsert' ? 'selected' : ''}>Upsert</option>
                </select>
            </div>
        </div>
        
        <!-- Source Configuration -->
        <div class="row mb-3">
            <div class="col-md-6">
                <h6><i class="fas fa-database"></i> Source</h6>
                <div class="mb-2">
                    <label class="form-label">Store Type</label>
                    <select class="form-select" onchange="updateSourceStore(${index}, this.value)">
                        ${Object.keys(storeTypes).map(type => 
                            `<option value="${type}" ${mapping.mapping.from.store.type === type ? 'selected' : ''}>${storeTypes[type].name}</option>`
                        ).join('')}
                    </select>
                </div>
                <div class="mb-2">
                    <label class="form-label">Data Format</label>
                    <select class="form-select" onchange="updateSourceFormat(${index}, this.value)">
                        ${Object.keys(dataFormats).map(format => 
                            `<option value="${format}" ${mapping.mapping.from.data_format.type === format ? 'selected' : ''}>${dataFormats[format].name}</option>`
                        ).join('')}
                    </select>
                </div>
                <div id="source-fields-${index}"></div>
            </div>
            
            <!-- Target Configuration -->
            <div class="col-md-6">
                <h6><i class="fas fa-file-export"></i> Target</h6>
                <div class="mb-2">
                    <label class="form-label">Store Type</label>
                    <select class="form-select" onchange="updateTargetStore(${index}, this.value)">
                        ${Object.keys(storeTypes).map(type => 
                            `<option value="${type}" ${mapping.mapping.to.store.type === type ? 'selected' : ''}>${storeTypes[type].name}</option>`
                        ).join('')}
                    </select>
                </div>
                <div class="mb-2">
                    <label class="form-label">Data Format</label>
                    <select class="form-select" onchange="updateTargetFormat(${index}, this.value)">
                        ${Object.keys(dataFormats).map(format => 
                            `<option value="${format}" ${mapping.mapping.to.data_format.type === format ? 'selected' : ''}>${dataFormats[format].name}</option>`
                        ).join('')}
                    </select>
                </div>
                <div id="target-fields-${index}"></div>
            </div>
        </div>
        
        <!-- Entities -->
        <div class="mb-3">
            <label class="form-label">Entities (tables/files to process)</label>
            <input type="text" class="form-control" placeholder="users, customers, orders (comma-separated)"
                   value="${mapping.mapping.from.entity.include.join(', ')}"
                   onchange="updateMappingEntities(${index}, this.value)">
        </div>
        
        <!-- Transformations -->
        <div class="mb-3">
            <div class="d-flex justify-content-between align-items-center mb-2">
                <label class="form-label">Transformations</label>
                <button type="button" class="btn btn-sm btn-outline-primary" onclick="addTransformation(${index})">
                    <i class="fas fa-plus"></i> Add Transformation
                </button>
            </div>
            <div id="transformations-${index}">
                <!-- Transformations will be rendered here -->
            </div>
        </div>
    `;
    
    container.appendChild(mappingDiv);
    renderStoreFields(index, 'source');
    renderStoreFields(index, 'target');
    renderTransformations(index);
}

// Render store-specific fields
function renderStoreFields(mappingIndex, storeType) {
    const mapping = currentConfig.mappings[mappingIndex];
    const store = storeType === 'source' ? mapping.mapping.from.store : mapping.mapping.to.store;
    const container = document.getElementById(`${storeType}-fields-${mappingIndex}`);
    
    if (!storeTypes[store.type]) return;
    
    const fields = storeTypes[store.type].fields;
    let html = '';
    
    fields.forEach(field => {
        if (field === 'password') {
            html += `
                <div class="mb-2">
                    <label class="form-label">${field.charAt(0).toUpperCase() + field.slice(1)}</label>
                    <input type="password" class="form-control" placeholder="${field}"
                           value="${store[field] || ''}"
                           onchange="updateStoreField(${mappingIndex}, '${storeType}', '${field}', this.value)">
                </div>
            `;
        } else {
            html += `
                <div class="mb-2">
                    <label class="form-label">${field.charAt(0).toUpperCase() + field.slice(1)}</label>
                    <input type="text" class="form-control" placeholder="${field}"
                           value="${store[field] || ''}"
                           onchange="updateStoreField(${mappingIndex}, '${storeType}', '${field}', this.value)">
                </div>
            `;
        }
    });
    
    container.innerHTML = html;
}

// Render transformations
function renderTransformations(mappingIndex) {
    const mapping = currentConfig.mappings[mappingIndex];
    const container = document.getElementById(`transformations-${mappingIndex}`);
    
    if (!mapping.mapping.transformations.length) {
        container.innerHTML = '<small class="text-muted">No transformations configured</small>';
        return;
    }
    
    let html = '';
    mapping.mapping.transformations.forEach((transform, transformIndex) => {
        html += `
            <div class="transformation-config mb-2">
                <div class="d-flex justify-content-between align-items-center mb-2">
                    <strong>${transformations[transform.type]?.name || transform.type}</strong>
                    <button type="button" class="btn btn-sm btn-outline-danger" 
                            onclick="removeTransformation(${mappingIndex}, ${transformIndex})">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
                <div class="transformation-fields">
                    <!-- Transformation-specific fields would go here -->
                    <small class="text-muted">Type: ${transform.type}</small>
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
}

// Update functions
function updateMappingField(index, field, value) {
    currentConfig.mappings[index].mapping[field] = value;
    updatePreview();
}

function updateSourceStore(index, storeType) {
    currentConfig.mappings[index].mapping.from.store = { type: storeType };
    renderStoreFields(index, 'source');
    updatePreview();
}

function updateTargetStore(index, storeType) {
    currentConfig.mappings[index].mapping.to.store = { type: storeType };
    renderStoreFields(index, 'target');
    updatePreview();
}

function updateSourceFormat(index, format) {
    currentConfig.mappings[index].mapping.from.data_format = { type: format };
    updatePreview();
}

function updateTargetFormat(index, format) {
    currentConfig.mappings[index].mapping.to.data_format = { type: format };
    updatePreview();
}

function updateStoreField(mappingIndex, storeType, field, value) {
    const store = storeType === 'source' 
        ? currentConfig.mappings[mappingIndex].mapping.from.store 
        : currentConfig.mappings[mappingIndex].mapping.to.store;
    store[field] = value;
    updatePreview();
}

function updateMappingEntities(index, entitiesStr) {
    const entities = entitiesStr.split(',').map(e => e.trim()).filter(e => e);
    currentConfig.mappings[index].mapping.from.entity.include = entities;
    updatePreview();
}

function addTransformation(mappingIndex) {
    const transformType = Object.keys(transformations)[0] || 'cleanup';
    currentConfig.mappings[mappingIndex].mapping.transformations.push({
        type: transformType
    });
    renderTransformations(mappingIndex);
    updatePreview();
}

function removeTransformation(mappingIndex, transformIndex) {
    currentConfig.mappings[mappingIndex].mapping.transformations.splice(transformIndex, 1);
    renderTransformations(mappingIndex);
    updatePreview();
}

function removeMapping(mappingId, index) {
    document.getElementById(`mapping-${mappingId}`).remove();
    currentConfig.mappings.splice(index, 1);
    updatePreview();
}

// Update YAML preview
async function updatePreview() {
    try {
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(currentConfig)
        });
        
        const yamlResponse = await fetch('/api/generate-yaml', {
            method: 'POST'
        });
        
        const yamlData = await yamlResponse.json();
        document.getElementById('yaml-preview').textContent = yamlData.yaml || '# No configuration yet';
    } catch (error) {
        console.error('Error updating preview:', error);
    }
}

// Validate configuration
async function validateConfig() {
    try {
        const response = await fetch('/api/validate', {
            method: 'POST'
        });
        
        const result = await response.json();
        const card = document.getElementById('validation-card');
        const container = document.getElementById('validation-results');
        
        let html = '';
        if (result.valid) {
            html = '<div class="alert alert-success"><i class="fas fa-check"></i> Configuration is valid!</div>';
        } else {
            html = '<div class="alert alert-danger"><strong>Validation Errors:</strong><ul>';
            result.errors.forEach(error => {
                html += `<li>${error}</li>`;
            });
            html += '</ul></div>';
        }
        
        if (result.warnings && result.warnings.length > 0) {
            html += '<div class="alert alert-warning"><strong>Warnings:</strong><ul>';
            result.warnings.forEach(warning => {
                html += `<li>${warning}</li>`;
            });
            html += '</ul></div>';
        }
        
        container.innerHTML = html;
        card.style.display = 'block';
    } catch (error) {
        console.error('Error validating config:', error);
    }
}

// Download YAML file
async function downloadYaml() {
    try {
        const response = await fetch('/api/download-yaml', {
            method: 'POST'
        });
        
        if (response.ok) {
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'pipeline_config.yaml';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);
        }
    } catch (error) {
        console.error('Error downloading YAML:', error);
    }
}

// Load YAML file
async function loadYamlFile() {
    const fileInput = document.getElementById('yaml-file');
    const file = fileInput.files[0];
    
    if (!file) return;
    
    const formData = new FormData();
    formData.append('file', file);
    
    try {
        const response = await fetch('/api/load-yaml', {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        if (result.status === 'success') {
            currentConfig = result.config;
            loadConfigurationToUI();
            updatePreview();
        } else {
            alert('Error loading file: ' + result.error);
        }
    } catch (error) {
        console.error('Error loading YAML file:', error);
    }
}

// Load configuration into UI
function loadConfigurationToUI() {
    // Load globals
    if (currentConfig.globals) {
        document.getElementById('global-env').value = currentConfig.globals.env || 'dev';
        document.getElementById('global-log-level').value = currentConfig.globals.log_level || 'INFO';
    }
    
    // Load pipeline
    if (currentConfig.pipeline) {
        document.getElementById('pipeline-name').value = currentConfig.pipeline.pipeline_name || '';
        document.getElementById('pipeline-platform').value = currentConfig.pipeline.platform?.type || 'local';
        document.getElementById('pipeline-engine').value = currentConfig.pipeline.engine?.type || 'python';
        document.getElementById('pipeline-retry-mode').value = currentConfig.pipeline.retry_mode || 'restart';
    }
    
    // Load mappings
    const container = document.getElementById('mappings-container');
    container.innerHTML = '';
    
    if (currentConfig.mappings) {
        currentConfig.mappings.forEach((mapping, index) => {
            const mappingId = Date.now() + index;
            renderMapping(mapping, mappingId, index);
        });
    }
}