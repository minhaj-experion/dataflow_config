"""
Pipeline execution orchestrator
"""

import time
from typing import Dict, Any, List
from ..logging.logger import Logger
from .engine import Engine


class Pipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self, config: Dict[str, Any], logger: Logger, retry_mode: str = "restart"):
        self.config = config
        self.logger = logger
        self.retry_mode = retry_mode
        self.engine = Engine(logger)
        
        # Extract pipeline information
        pipeline_config = config.get("pipeline", {})
        self.pipeline_name = pipeline_config.get("pipeline_name", "unknown")
        self.mappings = config.get("mappings", [])
        
        self.logger.info(f"Initialized pipeline: {self.pipeline_name}")
        self.logger.info(f"Retry mode: {retry_mode}")
        self.logger.info(f"Total mappings: {len(self.mappings)}")
    
    def execute(self) -> bool:
        """
        Execute the complete pipeline
        
        Returns:
            True if all mappings executed successfully, False otherwise
        """
        self.logger.info(f"Starting pipeline execution: {self.pipeline_name}")
        start_time = time.time()
        
        try:
            # Get the starting point based on retry mode
            start_index = self._get_start_index()
            
            # Execute mappings
            for i in range(start_index, len(self.mappings)):
                mapping = self.mappings[i]
                
                if "mapping" not in mapping:
                    self.logger.error(f"Invalid mapping structure at index {i}")
                    return False
                
                mapping_config = mapping["mapping"]
                mapping_name = mapping_config.get("mapping_name", f"mapping_{i}")
                
                self.logger.info(f"Processing mapping {i+1}/{len(self.mappings)}: {mapping_name}")
                
                success = self._execute_mapping_with_retry(mapping_config)
                
                if not success:
                    self.logger.error(f"Mapping failed: {mapping_name}")
                    return False
                
                # Log successful completion of mapping
                self._log_mapping_success(mapping_name, i)
            
            # Pipeline completed successfully
            execution_time = time.time() - start_time
            self.logger.info(f"Pipeline completed successfully: {self.pipeline_name}")
            self.logger.info(f"Total execution time: {execution_time:.2f} seconds")
            return True
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Pipeline failed: {self.pipeline_name}")
            self.logger.error(f"Error: {str(e)}")
            self.logger.error(f"Execution time before failure: {execution_time:.2f} seconds")
            return False
    
    def _get_start_index(self) -> int:
        """
        Get the starting index for pipeline execution based on retry mode
        
        Returns:
            Index to start execution from
        """
        if self.retry_mode == "restart":
            return 0
        elif self.retry_mode == "continue":
            # For MVP, always restart. In full implementation, this would check
            # the log store to find the last successful mapping
            self.logger.info("Continue mode - checking for last successful mapping")
            # TODO: Implement logic to check pipeline_log and mapping_log tables
            # For now, restart from beginning
            return 0
        else:
            self.logger.warning(f"Unknown retry mode: {self.retry_mode}, defaulting to restart")
            return 0
    
    def _execute_mapping_with_retry(self, mapping_config: Dict[str, Any]) -> bool:
        """
        Execute a mapping with basic retry logic
        
        Args:
            mapping_config: Mapping configuration
            
        Returns:
            True if successful, False otherwise
        """
        mapping_name = mapping_config.get("mapping_name", "unknown")
        max_retries = mapping_config.get("max_retries", 1)
        retry_delay = mapping_config.get("retry_delay", 5)  # seconds
        
        for attempt in range(max_retries):
            if attempt > 0:
                self.logger.info(f"Retrying mapping {mapping_name} (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            
            try:
                success = self.engine.execute_mapping(mapping_config)
                if success:
                    return True
                else:
                    self.logger.warning(f"Mapping {mapping_name} failed on attempt {attempt + 1}")
                    
            except Exception as e:
                self.logger.error(f"Error in mapping {mapping_name} on attempt {attempt + 1}: {str(e)}")
        
        self.logger.error(f"Mapping {mapping_name} failed after {max_retries} attempts")
        return False
    
    def _log_mapping_success(self, mapping_name: str, index: int):
        """Log successful completion of a mapping"""
        self.logger.info(f"Mapping completed successfully: {mapping_name}")
        # In full implementation, this would update the mapping_log table
        # For MVP, we just log to the file/console
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get current pipeline status
        
        Returns:
            Dictionary containing pipeline status information
        """
        return {
            "pipeline_name": self.pipeline_name,
            "total_mappings": len(self.mappings),
            "retry_mode": self.retry_mode,
            "config": self.config
        }
    
    def validate_pipeline(self) -> List[str]:
        """
        Validate pipeline configuration and dependencies
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check if pipeline has any mappings
        if not self.mappings:
            errors.append("Pipeline has no mappings defined")
        
        # Validate each mapping has required fields
        for i, mapping in enumerate(self.mappings):
            if "mapping" not in mapping:
                errors.append(f"Mapping {i+1} missing 'mapping' section")
                continue
            
            mapping_config = mapping["mapping"]
            
            # Check required fields
            required_fields = ["mapping_name", "from", "to"]
            for field in required_fields:
                if field not in mapping_config:
                    errors.append(f"Mapping {i+1} missing required field: {field}")
        
        return errors
