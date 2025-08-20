"""
Configuration parser for YAML files
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
from ..utils.helpers import substitute_variables


class ConfigParser:
    """Parse and process YAML configuration files"""
    
    def __init__(self):
        self.config = {}
    
    def parse(self, config_path: str) -> Dict[str, Any]:
        """
        Parse YAML configuration file with hierarchical structure
        
        Args:
            config_path: Path to the YAML configuration file
            
        Returns:
            Parsed configuration dictionary
        """
        config_file = Path(config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                raw_config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML syntax in config file: {e}")
        
        if not raw_config:
            raise ValueError("Configuration file is empty")
        
        # Process configuration hierarchy
        self.config = self._process_config(raw_config)
        
        # Substitute environment variables and dynamic references
        self.config = substitute_variables(self.config)
        
        return self.config
    
    def _process_config(self, raw_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process configuration with hierarchical inheritance
        
        Args:
            raw_config: Raw configuration from YAML file
            
        Returns:
            Processed configuration with inheritance applied
        """
        config = {
            "globals": raw_config.get("globals", {}),
            "pipeline": raw_config.get("pipeline", {}),
            "mappings": raw_config.get("mappings", [])
        }
        
        # Apply global defaults to pipeline level
        pipeline_config = {}
        pipeline_config.update(config["globals"])
        pipeline_config.update(config["pipeline"])
        config["pipeline"] = pipeline_config
        
        # Process mappings with inheritance
        processed_mappings = []
        for mapping in config["mappings"]:
            if "mapping" in mapping:
                processed_mapping = self._process_mapping(mapping["mapping"], config)
                processed_mappings.append({"mapping": processed_mapping})
            else:
                processed_mappings.append(mapping)
        
        config["mappings"] = processed_mappings
        
        return config
    
    def _process_mapping(self, mapping: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process individual mapping with inheritance from global and pipeline levels
        
        Args:
            mapping: Mapping configuration
            config: Full configuration context
            
        Returns:
            Processed mapping configuration
        """
        processed_mapping = {}
        
        # Apply inheritance: globals -> pipeline -> mapping
        processed_mapping.update(config["globals"])
        processed_mapping.update(config["pipeline"])
        processed_mapping.update(mapping)
        
        # Ensure required fields have defaults
        processed_mapping.setdefault("load_type", "full")
        processed_mapping.setdefault("write_mode", "overwrite")
        processed_mapping.setdefault("retry_mode", "restart")
        
        return processed_mapping
    
    def get_config(self) -> Dict[str, Any]:
        """Get the parsed configuration"""
        return self.config
    
    def get_pipeline_config(self) -> Dict[str, Any]:
        """Get pipeline-level configuration"""
        return self.config.get("pipeline", {})
    
    def get_mappings(self) -> list:
        """Get all mapping configurations"""
        return self.config.get("mappings", [])
    
    def get_globals(self) -> Dict[str, Any]:
        """Get global configuration"""
        return self.config.get("globals", {})
