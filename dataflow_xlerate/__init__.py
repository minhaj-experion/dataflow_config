"""
DataFlow xLerate - A declarative data ingestion framework
"""

__version__ = "1.0.0"
__author__ = "DataFlow Team"
__description__ = "A Python-based declarative data ingestion framework with YAML configuration"

from .core.pipeline import Pipeline
from .core.engine import Engine
from .config.parser import ConfigParser

__all__ = ["Pipeline", "Engine", "ConfigParser"]
