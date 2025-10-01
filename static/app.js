// DataFlow xLerate Configuration Builder with Visual Pipeline
let uiRegistry = {};
let cy = null; // Cytoscape instance
let currentConfig = {
    globals: {},
    pipeline: {},
    mappings: []
};

// Initialize the application
document.addEventListener('DOMContentLoaded', function() {
    loadUIRegistry();
    bindEvents();
    updatePreview();
    initializePipelineDiagram();
});

// Load UI registry with field definitions
async function loadUIRegistry() {
    try {
        const response = await fetch('/api/ui-registry');
        uiRegistry = await response.json();
        console.log('UI Registry loaded:', uiRegistry);
    } catch (error) {
        console.error('Error loading UI registry:', error);
    }
}

// Initialize Cytoscape pipeline diagram
function initializePipelineDiagram() {
    try {
        const container = document.getElementById('pipeline-diagram');
        
        // Wait for libraries to load
        if (typeof cytoscape === 'undefined') {
            setTimeout(initializePipelineDiagram, 100);
            return;
        }
        
        cy = cytoscape({
            container: container,
            style: [
                {
                    selector: 'node',
                    style: {
                        'background-color': '#007bff',
                        'label': 'data(label)',
                        'text-valign': 'center',
                        'text-halign': 'center',
                        'color': 'white',
                        'font-size': '12px',
                        'width': '120px',
                        'height': '40px',
                        'shape': 'roundrectangle',
                        'text-wrap': 'wrap',
                        'text-max-width': '100px'
                    }
                },
                {
                    selector: 'node.store',
                    style: {
                        'background-color': '#28a745',
                        'shape': 'ellipse'
                    }
                },
                {
                    selector: 'node.format',
                    style: {
                        'background-color': '#fd7e14',
                        'shape': 'rectangle'
                    }
                },
                {
                    selector: 'node.transformation',
                    style: {
                        'background-color': '#6f42c1',
                        'shape': 'diamond',
                        'width': '100px',
                        'height': '60px'
                    }
                },
                {
                    selector: 'edge',
                    style: {
                        'width': 3,
                        'line-color': '#ccc',
                        'target-arrow-color': '#ccc',
                        'target-arrow-shape': 'triangle',
                        'curve-style': 'bezier'
                    }
                }
            ],
            layout: {
                name: 'breadthfirst',
                directed: true,
                padding: 10,
                spacingFactor: 1.5
            },
            elements: []
        });
        
        // Try to register dagre layout if available
        if (typeof dagre !== 'undefined' && cy.dagre) {
            cytoscape.use(cy.dagre);
        }
        
        console.log('Pipeline diagram initialized successfully');
        
    } catch (error) {
        console.error('Error initializing pipeline diagram:', error);
        // Fallback: Just show a simple message
        const container = document.getElementById('pipeline-diagram');
        container.innerHTML = `
            <div class="text-center p-4 text-muted">
                <i class="fas fa-exclamation-triangle fa-2x mb-2"></i>
                <p>Visual pipeline diagram unavailable</p>
                <small>Using text-based configuration instead</small>
            </div>
        `;
    }
}

// Update visual pipeline diagram
function updatePipelineDiagram() {
    if (!cy) {
        return;
    }
    
    // If no mappings, show empty state
    if (!currentConfig.mappings.length) {
        cy.elements().remove();
        const container = document.getElementById('pipeline-diagram');
        const emptyState = container.querySelector('.text-center');
        if (!emptyState) {
            container.innerHTML = `
                <div class="text-center p-5 text-muted">
                    <i class="fas fa-project-diagram fa-3x mb-3"></i>
                    <p>Visual pipeline will appear here when you configure mappings</p>
                </div>
            `;
        }
        return;
    }
    
    // Clear empty state message
    const container = document.getElementById('pipeline-diagram');
    const emptyState = container.querySelector('.text-center');
    if (emptyState) {
        emptyState.remove();
    }
    
    const elements = [];
    let nodeId = 0;
    
    currentConfig.mappings.forEach((mappingWrapper, mappingIndex) => {
        const mapping = mappingWrapper.mapping;
        const prefix = `m${mappingIndex}_`;
        
        // Source store
        const sourceStoreId = `${prefix}source_store`;
        elements.push({
            data: { 
                id: sourceStoreId, 
                label: `${mapping.from?.store?.type || 'Store'}`,
                type: 'store'
            },
            classes: 'store'
        });
        
        // Source format
        const sourceFormatId = `${prefix}source_format`;
        elements.push({
            data: { 
                id: sourceFormatId, 
                label: `${mapping.from?.data_format?.type || 'Format'}`,
                type: 'format'
            },
            classes: 'format'
        });
        
        // Connect source store to format
        elements.push({
            data: { id: `${sourceStoreId}_to_${sourceFormatId}`, source: sourceStoreId, target: sourceFormatId }
        });
        
        let lastNodeId = sourceFormatId;
        
        // Transformations
        if (mapping.transformations && mapping.transformations.length > 0) {
            mapping.transformations.forEach((transform, transformIndex) => {
                const transformId = `${prefix}transform_${transformIndex}`;
                elements.push({
                    data: { 
                        id: transformId, 
                        label: transform.type || 'Transform',
                        type: 'transformation'
                    },
                    classes: 'transformation'
                });
                
                // Connect to transformation
                elements.push({
                    data: { id: `${lastNodeId}_to_${transformId}`, source: lastNodeId, target: transformId }
                });
                
                lastNodeId = transformId;
            });
        }
        
        // Target format
        const targetFormatId = `${prefix}target_format`;
        elements.push({
            data: { 
                id: targetFormatId, 
                label: `${mapping.to?.data_format?.type || 'Format'}`,
                type: 'format'
            },
            classes: 'format'
        });
        
        // Connect to target format
        elements.push({
            data: { id: `${lastNodeId}_to_${targetFormatId}`, source: lastNodeId, target: targetFormatId }
        });
        
        // Target store
        const targetStoreId = `${prefix}target_store`;
        elements.push({
            data: { 
                id: targetStoreId, 
                label: `${mapping.to?.store?.type || 'Store'}`,
                type: 'store'
            },
            classes: 'store'
        });
        
        // Connect format to target store
        elements.push({
            data: { id: `${targetFormatId}_to_${targetStoreId}`, source: targetFormatId, target: targetStoreId }
        });
    });
    
    // Update diagram
    cy.elements().remove();
    cy.add(elements);
    cy.layout({ 
        name: 'breadthfirst',
        directed: true,
        padding: 20,
        spacingFactor: 1.5,
        avoidOverlap: true
    }).run();
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
    updatePipelineDiagram();
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
    updatePipelineDiagram();
}

// Render mapping UI with dynamic fields
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
                        ${Object.keys(uiRegistry.stores || {}).map(type => 
                            `<option value="${type}" ${mapping.mapping.from.store.type === type ? 'selected' : ''}>${uiRegistry.stores[type].label}</option>`
                        ).join('')}
                    </select>
                </div>
                <div id="source-store-fields-${index}"></div>
                
                <div class="mb-2" id="source-format-selector-${index}">
                    <label class="form-label">Data Format</label>
                    <select class="form-select" onchange="updateSourceFormat(${index}, this.value)">
                        ${Object.keys(uiRegistry.formats || {}).map(format => 
                            `<option value="${format}" ${mapping.mapping.from.data_format.type === format ? 'selected' : ''}>${uiRegistry.formats[format].label}</option>`
                        ).join('')}
                    </select>
                </div>
                <div id="source-format-fields-${index}"></div>
            </div>
            
            <!-- Target Configuration -->
            <div class="col-md-6">
                <h6><i class="fas fa-file-export"></i> Target</h6>
                <div class="mb-2">
                    <label class="form-label">Store Type</label>
                    <select class="form-select" onchange="updateTargetStore(${index}, this.value)">
                        ${Object.keys(uiRegistry.stores || {}).map(type => 
                            `<option value="${type}" ${mapping.mapping.to.store.type === type ? 'selected' : ''}>${uiRegistry.stores[type].label}</option>`
                        ).join('')}
                    </select>
                </div>
                <div id="target-store-fields-${index}"></div>
                
                <div class="mb-2" id="target-format-selector-${index}">
                    <label class="form-label">Data Format</label>
                    <select class="form-select" onchange="updateTargetFormat(${index}, this.value)">
                        ${Object.keys(uiRegistry.formats || {}).map(format => 
                            `<option value="${format}" ${mapping.mapping.to.data_format.type === format ? 'selected' : ''}>${uiRegistry.formats[format].label}</option>`
                        ).join('')}
                    </select>
                </div>
                <div id="target-format-fields-${index}"></div>
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
    
    // Initialize format selector visibility based on store type
    const sourceFormatSelector = document.getElementById(`source-format-selector-${index}`);
    if (sourceFormatSelector && mapping.mapping.from.store.type !== 'local') {
        sourceFormatSelector.style.display = 'none';
    }
    
    const targetFormatSelector = document.getElementById(`target-format-selector-${index}`);
    if (targetFormatSelector && mapping.mapping.to.store.type !== 'local') {
        targetFormatSelector.style.display = 'none';
    }
    
    renderDynamicFields(index, 'source', 'store');
    renderDynamicFields(index, 'source', 'format');
    renderDynamicFields(index, 'target', 'store');
    renderDynamicFields(index, 'target', 'format');
    renderTransformations(index);
}

// Render dynamic fields based on selections
function renderDynamicFields(mappingIndex, context, fieldType) {
    const mapping = currentConfig.mappings[mappingIndex];
    const contextPath = context === 'source' ? 'from' : 'to';
    const type = fieldType === 'store' ? mapping.mapping[contextPath].store.type : mapping.mapping[contextPath].data_format.type;
    const registry = fieldType === 'store' ? uiRegistry.stores : uiRegistry.formats;
    const containerId = `${context}-${fieldType}-fields-${mappingIndex}`;
    const container = document.getElementById(containerId);
    
    if (!container || !registry || !registry[type]) {
        return;
    }
    
    const config = registry[type];
    let html = '';
    
    config.fields.forEach(field => {
        if (!shouldShowField(field, mapping.mapping, context)) {
            return;
        }
        
        html += renderField(field, mapping.mapping, mappingIndex, context);
    });
    
    container.innerHTML = html;
}

// Check if field should be shown based on show_if conditions
function shouldShowField(field, mappingConfig, context) {
    if (!field.show_if) {
        return true;
    }
    
    for (const [path, expectedValue] of Object.entries(field.show_if)) {
        const resolvedPath = path.replace('<ctx>', context === 'source' ? 'from' : 'to');
        const actualValue = getValueByPath(mappingConfig, resolvedPath);
        
        // Handle null comparison properly
        if (expectedValue === null && actualValue !== null) {
            return false;
        }
        if (expectedValue !== null && actualValue !== expectedValue) {
            return false;
        }
    }
    
    return true;
}

// Evaluate visibility for all fields and update DOM
function updateFieldVisibility(mappingIndex) {
    const mapping = currentConfig.mappings[mappingIndex];
    if (!mapping) return;
    
    // Update source store fields
    renderDynamicFields(mappingIndex, 'source', 'store');
    renderDynamicFields(mappingIndex, 'source', 'format');
    
    // Update target store fields
    renderDynamicFields(mappingIndex, 'target', 'store');
    renderDynamicFields(mappingIndex, 'target', 'format');
    
    updatePreview();
    updatePipelineDiagram();
}

// Get value by dot notation path
function getValueByPath(obj, path) {
    return path.split('.').reduce((current, key) => current && current[key], obj);
}

// Render individual field
function renderField(field, mappingConfig, mappingIndex, context) {
    const value = getValueByPath(mappingConfig, field.path.replace('<ctx>', context === 'source' ? 'from' : 'to')) || field.default || '';
    
    let fieldHtml = `<div class="field-group">`;
    fieldHtml += `<label class="form-label">${field.label}${field.required ? ' *' : ''}</label>`;
    
    switch (field.type) {
        case 'text':
        case 'number':
            fieldHtml += `<input type="${field.type}" class="form-control" placeholder="${field.placeholder || ''}" 
                         value="${value}" onchange="updateFieldValue(${mappingIndex}, '${field.path}', this.value, '${context}')">`;
            break;
        case 'secret':
            fieldHtml += `<input type="password" class="form-control" placeholder="${field.placeholder || ''}" 
                         value="${value}" onchange="updateFieldValue(${mappingIndex}, '${field.path}', this.value, '${context}')">`;
            break;
        case 'select':
            fieldHtml += `<select class="form-select" onchange="updateFieldValue(${mappingIndex}, '${field.path}', this.value, '${context}')">`;
            field.options.forEach(option => {
                fieldHtml += `<option value="${option.value}" ${value === option.value ? 'selected' : ''}>${option.label}</option>`;
            });
            fieldHtml += `</select>`;
            break;
        case 'boolean':
            fieldHtml += `<div class="form-check">
                <input class="form-check-input" type="checkbox" ${value ? 'checked' : ''} 
                       onchange="updateFieldValue(${mappingIndex}, '${field.path}', this.checked, '${context}')">
                <label class="form-check-label">${field.label}</label>
            </div>`;
            break;
        case 'code':
            fieldHtml += `<textarea class="form-control" rows="3" placeholder="${field.placeholder || ''}" 
                         onchange="updateFieldValue(${mappingIndex}, '${field.path}', this.value, '${context}')">${value}</textarea>`;
            break;
    }
    
    if (field.help) {
        fieldHtml += `<div class="field-help">${field.help}</div>`;
    }
    
    fieldHtml += `</div>`;
    return fieldHtml;
}

// Update field value in config
function updateFieldValue(mappingIndex, fieldPath, value, context) {
    const resolvedPath = fieldPath.replace('<ctx>', context === 'source' ? 'from' : 'to');
    const pathArray = resolvedPath.split('.');
    let current = currentConfig.mappings[mappingIndex].mapping;
    
    for (let i = 0; i < pathArray.length - 1; i++) {
        if (!current[pathArray[i]]) {
            current[pathArray[i]] = {};
        }
        current = current[pathArray[i]];
    }
    
    current[pathArray[pathArray.length - 1]] = value;
    
    // Trigger full field visibility update
    setTimeout(() => {
        updateFieldVisibility(mappingIndex);
    }, 50);
}

// Debounced update for better performance
let updateTimeout;
function debouncedUpdate(mappingIndex) {
    clearTimeout(updateTimeout);
    updateTimeout = setTimeout(() => {
        updateFieldVisibility(mappingIndex);
    }, 150);
}

// Update functions
function updateMappingField(index, field, value) {
    currentConfig.mappings[index].mapping[field] = value;
    debouncedUpdate(index);
}

function updateSourceStore(index, storeType) {
    currentConfig.mappings[index].mapping.from.store = { type: storeType };
    
    // Update data format based on store type
    // Only local storage has multiple format options; JDBC and API use their own format
    if (storeType === 'local') {
        // Default to CSV for local storage
        if (!currentConfig.mappings[index].mapping.from.data_format.type || 
            currentConfig.mappings[index].mapping.from.data_format.type === 'jdbc') {
            currentConfig.mappings[index].mapping.from.data_format = { type: 'csv' };
        }
    } else if (storeType === 'jdbc') {
        currentConfig.mappings[index].mapping.from.data_format = { type: 'jdbc' };
    } else if (storeType === 'api') {
        currentConfig.mappings[index].mapping.from.data_format = { type: 'json' };
    }
    
    // Show/hide format selector based on store type
    const formatSelector = document.getElementById(`source-format-selector-${index}`);
    if (formatSelector) {
        if (storeType === 'local') {
            formatSelector.style.display = 'block';
        } else {
            formatSelector.style.display = 'none';
        }
    }
    
    renderDynamicFields(index, 'source', 'store');
    renderDynamicFields(index, 'source', 'format');
    debouncedUpdate(index);
}

function updateTargetStore(index, storeType) {
    currentConfig.mappings[index].mapping.to.store = { type: storeType };
    
    // Update data format based on store type
    // Only local storage has multiple format options; JDBC and API use their own format
    if (storeType === 'local') {
        // Default to CSV for local storage
        if (!currentConfig.mappings[index].mapping.to.data_format.type || 
            currentConfig.mappings[index].mapping.to.data_format.type === 'jdbc') {
            currentConfig.mappings[index].mapping.to.data_format = { type: 'csv' };
        }
    } else if (storeType === 'jdbc') {
        currentConfig.mappings[index].mapping.to.data_format = { type: 'jdbc' };
    } else if (storeType === 'api') {
        currentConfig.mappings[index].mapping.to.data_format = { type: 'json' };
    }
    
    // Show/hide format selector based on store type
    const formatSelector = document.getElementById(`target-format-selector-${index}`);
    if (formatSelector) {
        if (storeType === 'local') {
            formatSelector.style.display = 'block';
        } else {
            formatSelector.style.display = 'none';
        }
    }
    
    renderDynamicFields(index, 'target', 'store');
    renderDynamicFields(index, 'target', 'format');
    debouncedUpdate(index);
}

function updateSourceFormat(index, format) {
    currentConfig.mappings[index].mapping.from.data_format = { type: format };
    debouncedUpdate(index);
}

function updateTargetFormat(index, format) {
    currentConfig.mappings[index].mapping.to.data_format = { type: format };
    debouncedUpdate(index);
}

function updateMappingEntities(index, entitiesStr) {
    const entities = entitiesStr.split(',').map(e => e.trim()).filter(e => e);
    currentConfig.mappings[index].mapping.from.entity.include = entities;
    debouncedUpdate(index);
}

function addTransformation(mappingIndex) {
    const transformType = Object.keys(uiRegistry.transformations || {})[0] || 'cleanup';
    currentConfig.mappings[mappingIndex].mapping.transformations.push({
        type: transformType
    });
    renderTransformations(mappingIndex);
    debouncedUpdate(mappingIndex);
}

function removeTransformation(mappingIndex, transformIndex) {
    currentConfig.mappings[mappingIndex].mapping.transformations.splice(transformIndex, 1);
    renderTransformations(mappingIndex);
    debouncedUpdate(mappingIndex);
}

function removeMapping(mappingId, index) {
    document.getElementById(`mapping-${mappingId}`).remove();
    currentConfig.mappings.splice(index, 1);
    updatePreview();
    updatePipelineDiagram();
    
    // Re-index remaining mappings
    currentConfig.mappings.forEach((mapping, newIndex) => {
        const mappingElement = document.querySelector(`[id^="mapping-"]`);
        if (mappingElement) {
            const title = mappingElement.querySelector('h6');
            if (title) {
                title.innerHTML = `<i class="fas fa-exchange-alt"></i> Mapping ${newIndex + 1}`;
            }
        }
    });
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
        const transformConfig = uiRegistry.transformations[transform.type];
        html += `
            <div class="transformation-config mb-2">
                <div class="d-flex justify-content-between align-items-center mb-2">
                    <strong><i class="${transformConfig?.icon || 'fas fa-cog'}"></i> ${transformConfig?.label || transform.type}</strong>
                    <button type="button" class="btn btn-sm btn-outline-danger" 
                            onclick="removeTransformation(${mappingIndex}, ${transformIndex})">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
                <div class="transformation-fields">
                    <small class="text-muted">${transformConfig?.description || ''}</small>
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
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
            updatePipelineDiagram();
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