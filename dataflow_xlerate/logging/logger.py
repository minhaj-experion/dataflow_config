"""
Structured logging framework for DataFlow xLerate
"""

import logging
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any


class Logger:
    """Structured logger for DataFlow xLerate"""
    
    def __init__(self, log_level: str = "INFO", log_dir: str = "logs"):
        self.log_level = log_level.upper()
        self.log_dir = Path(log_dir)
        self.logger = None
        self._setup_logger()
    
    def _setup_logger(self):
        """Setup the logging configuration"""
        # Create log directory if it doesn't exist
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger("dataflow_xlerate")
        self.logger.setLevel(getattr(logging, self.log_level))
        
        # Clear any existing handlers
        self.logger.handlers.clear()
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        console_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, self.log_level))
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler
        log_filename = f"dataflow_xlerate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_filepath = self.log_dir / log_filename
        
        file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)  # Always log everything to file
        file_handler.setFormatter(detailed_formatter)
        self.logger.addHandler(file_handler)
        
        # Log initialization
        self.info(f"Logger initialized - Level: {self.log_level}, Log file: {log_filepath}")
    
    def debug(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """Log debug message"""
        self._log(logging.DEBUG, message, extra)
    
    def info(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """Log info message"""
        self._log(logging.INFO, message, extra)
    
    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """Log warning message"""
        self._log(logging.WARNING, message, extra)
    
    def error(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """Log error message"""
        self._log(logging.ERROR, message, extra)
    
    def critical(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """Log critical message"""
        self._log(logging.CRITICAL, message, extra)
    
    def _log(self, level: int, message: str, extra: Optional[Dict[str, Any]] = None):
        """Internal logging method"""
        if self.logger is None:
            print(f"Logger not initialized: {message}")
            return
            
        if extra:
            # Format extra information
            extra_str = " | ".join([f"{k}={v}" for k, v in extra.items()])
            full_message = f"{message} | {extra_str}"
        else:
            full_message = message
        
        self.logger.log(level, full_message)
    
    def log_pipeline_start(self, pipeline_name: str, config: Dict[str, Any]):
        """Log pipeline start event"""
        self.info(
            f"Pipeline started: {pipeline_name}",
            {
                "event_type": "pipeline_start",
                "pipeline_name": pipeline_name,
                "mappings_count": len(config.get("mappings", []))
            }
        )
    
    def log_pipeline_end(self, pipeline_name: str, success: bool, duration: float):
        """Log pipeline end event"""
        status = "success" if success else "failure"
        self.info(
            f"Pipeline {status}: {pipeline_name} (duration: {duration:.2f}s)",
            {
                "event_type": "pipeline_end",
                "pipeline_name": pipeline_name,
                "status": status,
                "duration": duration
            }
        )
    
    def log_mapping_start(self, mapping_name: str):
        """Log mapping start event"""
        self.info(
            f"Mapping started: {mapping_name}",
            {
                "event_type": "mapping_start",
                "mapping_name": mapping_name
            }
        )
    
    def log_mapping_end(self, mapping_name: str, success: bool, rows_processed: int = 0):
        """Log mapping end event"""
        status = "success" if success else "failure"
        self.info(
            f"Mapping {status}: {mapping_name} (rows: {rows_processed})",
            {
                "event_type": "mapping_end",
                "mapping_name": mapping_name,
                "status": status,
                "rows_processed": rows_processed
            }
        )
    
    def log_entity_processing(self, entity: str, operation: str, rows: int = 0):
        """Log entity processing event"""
        self.info(
            f"Entity {operation}: {entity} (rows: {rows})",
            {
                "event_type": "entity_processing",
                "entity": entity,
                "operation": operation,
                "rows": rows
            }
        )
    
    def get_log_dir(self) -> Path:
        """Get the log directory path"""
        return self.log_dir
    
    def set_level(self, level: str):
        """Change logging level"""
        self.log_level = level.upper()
        if self.logger is None:
            return
        self.logger.setLevel(getattr(logging, self.log_level))
        for handler in self.logger.handlers:
            if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
                handler.setLevel(getattr(logging, self.log_level))
