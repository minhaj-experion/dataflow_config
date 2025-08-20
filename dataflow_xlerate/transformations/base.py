"""
Base transformation interface and factory
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any
from ..logging.logger import Logger


class BaseTransformation(ABC):
    """Abstract base class for all transformations"""
    
    def __init__(self, config: Dict[str, Any], logger: Logger):
        self.config = config
        self.logger = logger
    
    @abstractmethod
    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformation to the data
        
        Args:
            data: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        pass
    
    def validate_config(self) -> bool:
        """
        Validate transformation configuration
        
        Returns:
            True if configuration is valid, False otherwise
        """
        return True


class TransformationFactory:
    """Factory for creating transformation instances"""
    
    def __init__(self, logger: Logger):
        self.logger = logger
    
    def create_transformation(self, config: Dict[str, Any]) -> BaseTransformation:
        """
        Create a transformation instance based on configuration
        
        Args:
            config: Transformation configuration dictionary
            
        Returns:
            Transformation instance
            
        Raises:
            ValueError: If transformation type is not supported
        """
        transformation_type = config.get("type")
        
        if not transformation_type:
            raise ValueError("Transformation configuration missing 'type' field")
        
        if transformation_type == "schema_map":
            from .basic import SchemaMapTransformation
            return SchemaMapTransformation(config, self.logger)
        elif transformation_type == "filter":
            from .basic import FilterTransformation
            return FilterTransformation(config, self.logger)
        elif transformation_type == "cleanup":
            from .basic import BasicCleanupTransformation
            return BasicCleanupTransformation(config, self.logger)
        else:
            raise ValueError(f"Unsupported transformation type: {transformation_type}")
