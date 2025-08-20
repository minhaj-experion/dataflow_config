"""
File system store implementation for local and cloud storage
"""

import pandas as pd
import os
from pathlib import Path
from typing import Dict, Any, Optional
from .base import BaseStore


class LocalFileStore(BaseStore):
    """Local file system store implementation"""
    
    def __init__(self, config: Dict[str, Any], logger):
        super().__init__(config, logger)
        self.base_path = Path(self.store_config.get("path", "./data"))
        self.data_format = self.data_format_config.get("type", "csv")
        self._ensure_base_path()
    
    def _ensure_base_path(self):
        """Ensure the base path exists"""
        try:
            self.base_path.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Base path ready: {self.base_path}")
        except Exception as e:
            self.logger.error(f"Failed to create base path {self.base_path}: {str(e)}")
            raise
    
    def _get_entity_path(self, entity: str) -> Path:
        """Get the full path for an entity file"""
        # Substitute entity name in path template
        path_str = str(self.base_path)
        if "{entity}" in path_str:
            path_str = path_str.replace("{entity}", entity)
        elif "${entity}$" in path_str:
            path_str = path_str.replace("${entity}$", entity)
        
        entity_path = Path(path_str)
        
        # If path is a directory, append filename
        if not entity_path.suffix:
            filename = f"{entity}.{self.data_format}"
            entity_path = entity_path / filename
        
        return entity_path
    
    def read_entity(self, entity: str) -> Optional[pd.DataFrame]:
        """Read data from a file"""
        try:
            entity_path = self._get_entity_path(entity)
            
            if not entity_path.exists():
                self.logger.warning(f"File does not exist: {entity_path}")
                return None
            
            self.logger.info(f"Reading data from file: {entity_path}")
            
            # Read based on format
            if self.data_format.lower() == "csv":
                df = pd.read_csv(entity_path)
            elif self.data_format.lower() == "parquet":
                df = pd.read_parquet(entity_path)
            elif self.data_format.lower() == "json":
                df = pd.read_json(entity_path)
            else:
                raise ValueError(f"Unsupported file format: {self.data_format}")
            
            self.logger.info(f"Successfully read {len(df)} rows from file: {entity_path}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading file for entity {entity}: {str(e)}")
            return None
    
    def write_entity(self, entity: str, data: pd.DataFrame, write_mode: str = "overwrite") -> bool:
        """Write data to a file"""
        try:
            entity_path = self._get_entity_path(entity)
            
            # Ensure parent directory exists
            entity_path.parent.mkdir(parents=True, exist_ok=True)
            
            self.logger.info(f"Writing {len(data)} rows to file: {entity_path} (mode: {write_mode})")
            
            # Handle write modes
            if write_mode == "overwrite":
                # Simply overwrite the file
                pass
            elif write_mode == "append":
                # Read existing data and append
                if entity_path.exists():
                    existing_data = self.read_entity(entity)
                    if existing_data is not None:
                        data = pd.concat([existing_data, data], ignore_index=True)
            elif write_mode in ["upsert", "upsert_only"]:
                # For MVP, treat as overwrite
                self.logger.warning(f"Upsert mode not fully implemented, using overwrite for entity: {entity}")
            else:
                self.logger.error(f"Unsupported write mode: {write_mode}")
                return False
            
            # Write based on format
            if self.data_format.lower() == "csv":
                data.to_csv(entity_path, index=False)
            elif self.data_format.lower() == "parquet":
                data.to_parquet(entity_path, index=False)
            elif self.data_format.lower() == "json":
                data.to_json(entity_path, orient='records', indent=2)
            else:
                raise ValueError(f"Unsupported file format: {self.data_format}")
            
            self.logger.info(f"Successfully wrote data to file: {entity_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing file for entity {entity}: {str(e)}")
            return False
    
    def list_entities(self) -> list:
        """List all files in the base directory"""
        try:
            if not self.base_path.exists():
                return []
            
            entities = []
            for file_path in self.base_path.glob(f"*.{self.data_format}"):
                entity_name = file_path.stem
                entities.append(entity_name)
            
            self.logger.info(f"Found {len(entities)} files in {self.base_path}")
            return entities
            
        except Exception as e:
            self.logger.error(f"Error listing files: {str(e)}")
            return []
    
    def entity_exists(self, entity: str) -> bool:
        """Check if a file exists"""
        try:
            entity_path = self._get_entity_path(entity)
            return entity_path.exists()
            
        except Exception as e:
            self.logger.error(f"Error checking if file {entity} exists: {str(e)}")
            return False
    
    def get_connection_info(self) -> str:
        """Get connection info for logging"""
        return f"Local file store: {self.base_path} (format: {self.data_format})"
