"""
Base store interface and factory
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any, Optional
from ..logging.logger import Logger


class BaseStore(ABC):
    """Abstract base class for all store implementations"""
    
    def __init__(self, config: Dict[str, Any], logger: Logger):
        self.config = config
        self.logger = logger
        self.store_config = config.get("store", {})
        self.data_format_config = config.get("data_format", {})
    
    @abstractmethod
    def read_entity(self, entity: str) -> Optional[pd.DataFrame]:
        """
        Read data for a specific entity
        
        Args:
            entity: Entity name (e.g., table name, file name)
            
        Returns:
            DataFrame containing the data, or None if no data found
        """
        pass
    
    @abstractmethod
    def write_entity(self, entity: str, data: pd.DataFrame, write_mode: str = "overwrite") -> bool:
        """
        Write data for a specific entity
        
        Args:
            entity: Entity name
            data: DataFrame to write
            write_mode: Write mode (overwrite, append, upsert, etc.)
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def list_entities(self) -> list:
        """
        List all available entities in this store
        
        Returns:
            List of entity names
        """
        pass
    
    @abstractmethod
    def entity_exists(self, entity: str) -> bool:
        """
        Check if an entity exists in the store
        
        Args:
            entity: Entity name to check
            
        Returns:
            True if entity exists, False otherwise
        """
        pass
    
    def get_connection_info(self) -> str:
        """Get a string representation of connection info for logging"""
        store_type = self.store_config.get("type", "unknown")
        return f"{store_type} store"


class StoreFactory:
    """Factory for creating store instances"""
    
    def __init__(self, logger: Logger):
        self.logger = logger
    
    def create_store(self, config: Dict[str, Any]) -> BaseStore:
        """
        Create a store instance based on configuration
        
        Args:
            config: Store configuration dictionary
            
        Returns:
            Store instance
            
        Raises:
            ValueError: If store type is not supported
        """
        store_config = config.get("store", {})
        store_type = store_config.get("type")
        
        if not store_type:
            raise ValueError("Store configuration missing 'type' field")
        
        if store_type == "jdbc":
            from .jdbc import JDBCStore
            return JDBCStore(config, self.logger)
        elif store_type == "local":
            from .filesystem import LocalFileStore
            return LocalFileStore(config, self.logger)
        elif store_type in ["s3", "hdfs"]:
            # Placeholder for future implementation
            raise ValueError(f"Store type '{store_type}' not yet implemented")
        else:
            raise ValueError(f"Unsupported store type: {store_type}")
