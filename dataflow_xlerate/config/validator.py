"""
Configuration validator for DataFlow xLerate
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class ValidationResult:
    """Result of configuration validation"""
    is_valid: bool
    errors: List[str]
    warnings: Optional[List[str]] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


class ConfigValidator:
    """Validate configuration structure and values"""
    
    REQUIRED_PIPELINE_FIELDS = ["pipeline_name"]
    REQUIRED_MAPPING_FIELDS = ["mapping_name", "from", "to"]
    SUPPORTED_STORE_TYPES = ["jdbc", "local", "s3", "hdfs"]
    SUPPORTED_DATA_FORMATS = ["csv", "parquet", "json", "jdbc"]
    SUPPORTED_LOAD_TYPES = ["full", "incremental"]
    SUPPORTED_WRITE_MODES = ["overwrite", "append", "upsert", "upsert_only", "append_delete"]
    SUPPORTED_ENGINE_TYPES = ["python"]
    
    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Validate the entire configuration
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            ValidationResult with validation status and any errors
        """
        errors = []
        warnings = []
        
        # Validate top-level structure
        if not isinstance(config, dict):
            errors.append("Configuration must be a dictionary")
            return ValidationResult(False, errors, warnings)
        
        # Validate pipeline section
        pipeline_errors = self._validate_pipeline(config.get("pipeline", {}))
        errors.extend(pipeline_errors)
        
        # Validate mappings section
        mappings_errors, mappings_warnings = self._validate_mappings(config.get("mappings", []))
        errors.extend(mappings_errors)
        warnings.extend(mappings_warnings)
        
        # Validate globals section
        globals_warnings = self._validate_globals(config.get("globals", {}))
        warnings.extend(globals_warnings)
        
        return ValidationResult(len(errors) == 0, errors, warnings)
    
    def _validate_pipeline(self, pipeline: Dict[str, Any]) -> List[str]:
        """Validate pipeline configuration"""
        errors = []
        
        if not isinstance(pipeline, dict):
            errors.append("Pipeline section must be a dictionary")
            return errors
        
        # Check required fields
        for field in self.REQUIRED_PIPELINE_FIELDS:
            if field not in pipeline:
                errors.append(f"Pipeline section missing required field: {field}")
        
        # Validate engine type if specified
        if "engine" in pipeline:
            engine = pipeline["engine"]
            if isinstance(engine, dict):
                engine_type = engine.get("type")
                if engine_type and engine_type not in self.SUPPORTED_ENGINE_TYPES:
                    errors.append(f"Unsupported engine type: {engine_type}. Supported: {self.SUPPORTED_ENGINE_TYPES}")
        
        return errors
    
    def _validate_mappings(self, mappings: List[Dict[str, Any]]) -> tuple[List[str], List[str]]:
        """Validate mappings configuration"""
        errors = []
        warnings = []
        
        if not isinstance(mappings, list):
            errors.append("Mappings section must be a list")
            return errors, warnings
        
        if len(mappings) == 0:
            warnings.append("No mappings defined in configuration")
            return errors, warnings
        
        for i, mapping_wrapper in enumerate(mappings):
            if not isinstance(mapping_wrapper, dict) or "mapping" not in mapping_wrapper:
                errors.append(f"Mapping {i+1} must be a dictionary with 'mapping' key")
                continue
            
            mapping = mapping_wrapper["mapping"]
            mapping_errors, mapping_warnings = self._validate_single_mapping(mapping, i+1)
            errors.extend(mapping_errors)
            warnings.extend(mapping_warnings)
        
        return errors, warnings
    
    def _validate_single_mapping(self, mapping: Dict[str, Any], index: int) -> tuple[List[str], List[str]]:
        """Validate a single mapping configuration"""
        errors = []
        warnings = []
        
        if not isinstance(mapping, dict):
            errors.append(f"Mapping {index} must be a dictionary")
            return errors, warnings
        
        # Check required fields
        for field in self.REQUIRED_MAPPING_FIELDS:
            if field not in mapping:
                errors.append(f"Mapping {index} missing required field: {field}")
        
        # Validate load_type
        load_type = mapping.get("load_type", "full")
        if load_type not in self.SUPPORTED_LOAD_TYPES:
            errors.append(f"Mapping {index} has unsupported load_type: {load_type}. Supported: {self.SUPPORTED_LOAD_TYPES}")
        
        # Validate write_mode
        write_mode = mapping.get("write_mode", "overwrite")
        if write_mode not in self.SUPPORTED_WRITE_MODES:
            errors.append(f"Mapping {index} has unsupported write_mode: {write_mode}. Supported: {self.SUPPORTED_WRITE_MODES}")
        
        # Validate from store
        if "from" in mapping:
            from_errors = self._validate_store_config(mapping["from"], f"Mapping {index} 'from'")
            errors.extend(from_errors)
        
        # Validate to store
        if "to" in mapping:
            to_errors = self._validate_store_config(mapping["to"], f"Mapping {index} 'to'")
            errors.extend(to_errors)
        
        # Incremental load warnings
        if load_type == "incremental":
            warnings.append(f"Mapping {index} uses incremental load - ensure proper offset configuration")
        
        return errors, warnings
    
    def _validate_store_config(self, store_config: Dict[str, Any], context: str) -> List[str]:
        """Validate store configuration"""
        errors = []
        
        if not isinstance(store_config, dict):
            errors.append(f"{context} store configuration must be a dictionary")
            return errors
        
        # Validate store section
        if "store" not in store_config:
            errors.append(f"{context} missing 'store' section")
            return errors
        
        store = store_config["store"]
        if not isinstance(store, dict):
            errors.append(f"{context} 'store' section must be a dictionary")
            return errors
        
        # Validate store type
        store_type = store.get("type")
        if not store_type:
            errors.append(f"{context} store missing 'type' field")
        elif store_type not in self.SUPPORTED_STORE_TYPES:
            errors.append(f"{context} has unsupported store type: {store_type}. Supported: {self.SUPPORTED_STORE_TYPES}")
        
        # Validate data format if present
        if "data_format" in store_config:
            data_format = store_config["data_format"]
            if isinstance(data_format, dict):
                format_type = data_format.get("type")
                if format_type and format_type not in self.SUPPORTED_DATA_FORMATS:
                    errors.append(f"{context} has unsupported data format: {format_type}. Supported: {self.SUPPORTED_DATA_FORMATS}")
        
        # JDBC-specific validation
        if store_type == "jdbc":
            self._validate_jdbc_store(store, context, errors)
        
        # Local file system validation
        elif store_type == "local":
            self._validate_local_store(store, context, errors)
        
        return errors
    
    def _validate_jdbc_store(self, store: Dict[str, Any], context: str, errors: List[str]):
        """Validate JDBC store configuration"""
        if "connection_url" not in store and "db_name" not in store:
            errors.append(f"{context} JDBC store must have either 'connection_url' or 'db_name'")
    
    def _validate_local_store(self, store: Dict[str, Any], context: str, errors: List[str]):
        """Validate local file system store configuration"""
        if "path" not in store:
            errors.append(f"{context} local store must have 'path' field")
    
    def _validate_globals(self, globals_config: Dict[str, Any]) -> List[str]:
        """Validate globals configuration"""
        warnings = []
        
        if not isinstance(globals_config, dict):
            warnings.append("Globals section should be a dictionary")
        
        return warnings
