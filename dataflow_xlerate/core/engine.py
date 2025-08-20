"""
Core execution engine for data processing
"""

import pandas as pd
from typing import Dict, Any, List, Optional
from ..stores.base import StoreFactory
from ..transformations.base import TransformationFactory
from ..logging.logger import Logger


class Engine:
    """Core execution engine for data processing operations"""
    
    def __init__(self, logger: Logger):
        self.logger = logger
        self.store_factory = StoreFactory(logger)
        self.transformation_factory = TransformationFactory(logger)
    
    def execute_mapping(self, mapping_config: Dict[str, Any]) -> bool:
        """
        Execute a single mapping (data movement from source to target)
        
        Args:
            mapping_config: Mapping configuration dictionary
            
        Returns:
            True if successful, False otherwise
        """
        mapping_name = mapping_config.get("mapping_name", "unknown")
        self.logger.info(f"Executing mapping: {mapping_name}")
        
        try:
            # Get source and target store configurations
            from_config = mapping_config["from"]
            to_config = mapping_config["to"]
            
            # Create store instances
            source_store = self.store_factory.create_store(from_config)
            target_store = self.store_factory.create_store(to_config)
            
            # Get entities to process
            entities = self._get_entities(from_config)
            if not entities:
                self.logger.warning(f"No entities found for mapping: {mapping_name}")
                return True
            
            # Process each entity
            for entity in entities:
                success = self._process_entity(
                    entity, mapping_config, source_store, target_store
                )
                if not success:
                    self.logger.error(f"Failed to process entity: {entity}")
                    return False
            
            self.logger.info(f"Mapping completed successfully: {mapping_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error executing mapping {mapping_name}: {str(e)}")
            return False
    
    def _get_entities(self, from_config: Dict[str, Any]) -> List[str]:
        """Get list of entities to process from source configuration"""
        entity_config = from_config.get("entity", {})
        
        # Get included entities
        included = entity_config.get("include", [])
        if included:
            return included
        
        # If no specific entities listed, try to discover from source
        # This would be implemented based on store type
        return []
    
    def _process_entity(
        self,
        entity: str,
        mapping_config: Dict[str, Any],
        source_store,
        target_store
    ) -> bool:
        """
        Process a single entity (e.g., table, file)
        
        Args:
            entity: Entity name to process
            mapping_config: Mapping configuration
            source_store: Source store instance
            target_store: Target store instance
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Processing entity: {entity}")
        
        try:
            # Extract data from source
            data = self._extract_data(entity, mapping_config, source_store)
            if data is None or data.empty:
                self.logger.warning(f"No data extracted for entity: {entity}")
                return True
            
            self.logger.info(f"Extracted {len(data)} rows for entity: {entity}")
            
            # Apply transformations
            transformed_data = self._apply_transformations(data, mapping_config, entity)
            
            # Load data to target
            success = self._load_data(transformed_data, entity, mapping_config, target_store)
            
            if success:
                self.logger.info(f"Successfully processed entity: {entity}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error processing entity {entity}: {str(e)}")
            return False
    
    def _extract_data(
        self,
        entity: str,
        mapping_config: Dict[str, Any],
        source_store
    ) -> Optional[pd.DataFrame]:
        """Extract data from source store"""
        try:
            load_type = mapping_config.get("load_type", "full")
            
            if load_type == "full":
                return source_store.read_entity(entity)
            elif load_type == "incremental":
                # For now, fall back to full load
                # Incremental load logic would be implemented here
                self.logger.warning(f"Incremental load not yet implemented, using full load for {entity}")
                return source_store.read_entity(entity)
            else:
                raise ValueError(f"Unsupported load type: {load_type}")
                
        except Exception as e:
            self.logger.error(f"Error extracting data for entity {entity}: {str(e)}")
            return None
    
    def _apply_transformations(
        self,
        data: pd.DataFrame,
        mapping_config: Dict[str, Any],
        entity: str
    ) -> pd.DataFrame:
        """Apply transformations to the data"""
        try:
            # Get transformation configuration
            transformations = mapping_config.get("transformations", [])
            
            transformed_data = data.copy()
            
            for transformation_config in transformations:
                transformation = self.transformation_factory.create_transformation(transformation_config)
                transformed_data = transformation.apply(transformed_data)
            
            # Apply default transformations if none specified
            if not transformations:
                # Basic schema mapping and cleanup
                transformed_data = self._apply_default_transformations(transformed_data, entity)
            
            self.logger.info(f"Applied transformations to {len(transformed_data)} rows for entity: {entity}")
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"Error applying transformations for entity {entity}: {str(e)}")
            return data  # Return original data if transformations fail
    
    def _apply_default_transformations(self, data: pd.DataFrame, entity: str) -> pd.DataFrame:
        """Apply default transformations (basic cleanup)"""
        # Remove completely empty rows
        data = data.dropna(how='all')
        
        # Basic column name cleanup (remove extra spaces, etc.)
        data.columns = data.columns.str.strip()
        
        return data
    
    def _load_data(
        self,
        data: pd.DataFrame,
        entity: str,
        mapping_config: Dict[str, Any],
        target_store
    ) -> bool:
        """Load data to target store"""
        try:
            write_mode = mapping_config.get("write_mode", "overwrite")
            
            success = target_store.write_entity(entity, data, write_mode)
            
            if success:
                self.logger.info(f"Successfully wrote {len(data)} rows for entity: {entity}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error loading data for entity {entity}: {str(e)}")
            return False
