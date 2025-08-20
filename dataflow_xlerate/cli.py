"""
Command Line Interface for DataFlow xLerate
"""

import click
import sys
import os
import traceback
from pathlib import Path
from typing import Optional

from .config.parser import ConfigParser
from .config.validator import ConfigValidator
from .core.pipeline import Pipeline
from .logging.logger import Logger


@click.command()
@click.option(
    "--config", "-c",
    required=True,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True),
    help="Path to the YAML configuration file"
)
@click.option(
    "--pipeline-name", "-p",
    help="Name of the pipeline to execute (if not specified, uses pipeline name from config)"
)
@click.option(
    "--retry-mode",
    type=click.Choice(["restart", "continue"], case_sensitive=False),
    default="restart",
    help="Retry mode: 'restart' (reprocess entire pipeline) or 'continue' (resume from last successful mapping)"
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    default="INFO",
    help="Logging level"
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Validate configuration and show execution plan without running pipeline"
)
@click.version_option(version="1.0.0", prog_name="DataFlow xLerate")
def main(config: str, pipeline_name: Optional[str], retry_mode: str, log_level: str, dry_run: bool):
    """
    DataFlow xLerate - Declarative Data Ingestion Framework
    
    Execute data pipelines defined in YAML configuration files.
    """
    try:
        # Initialize logger
        logger = Logger(log_level=log_level.upper())
        logger.info("Starting DataFlow xLerate")
        logger.info(f"Configuration file: {config}")
        logger.info(f"Retry mode: {retry_mode}")
        logger.info(f"Log level: {log_level}")
        
        # Parse configuration
        config_parser = ConfigParser()
        pipeline_config = config_parser.parse(config)
        
        # Override pipeline name if provided via CLI
        if pipeline_name:
            pipeline_config["pipeline"]["pipeline_name"] = pipeline_name
            
        # Validate configuration
        validator = ConfigValidator()
        validation_result = validator.validate(pipeline_config)
        
        if not validation_result.is_valid:
            logger.error("Configuration validation failed:")
            for error in validation_result.errors:
                logger.error(f"  - {error}")
            sys.exit(1)
            
        logger.info("Configuration validation passed")
        
        if dry_run:
            logger.info("Dry run mode - showing execution plan:")
            _show_execution_plan(pipeline_config, logger)
            return
            
        # Create and execute pipeline
        pipeline = Pipeline(pipeline_config, logger, retry_mode)
        success = pipeline.execute()
        
        if success:
            logger.info("Pipeline execution completed successfully")
            sys.exit(0)
        else:
            logger.error("Pipeline execution failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"Fatal error: {str(e)}", file=sys.stderr)
        print("Traceback:", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


def _show_execution_plan(config: dict, logger: Logger):
    """Show the execution plan for dry run mode"""
    pipeline_name = config["pipeline"]["pipeline_name"]
    logger.info(f"Pipeline: {pipeline_name}")
    
    mappings = config.get("mappings", [])
    logger.info(f"Total mappings: {len(mappings)}")
    
    for i, mapping in enumerate(mappings, 1):
        mapping_info = mapping["mapping"]
        mapping_name = mapping_info["mapping_name"]
        load_type = mapping_info.get("load_type", "full")
        write_mode = mapping_info.get("write_mode", "overwrite")
        
        logger.info(f"  {i}. Mapping: {mapping_name}")
        logger.info(f"     Load Type: {load_type}")
        logger.info(f"     Write Mode: {write_mode}")
        
        # Source info
        from_store = mapping_info["from"]["store"]
        logger.info(f"     Source: {from_store['type']}")
        
        # Target info
        to_store = mapping_info["to"]["store"]
        logger.info(f"     Target: {to_store['type']}")
        
        # Entities
        entities = mapping_info["from"].get("entity", {}).get("include", [])
        if entities:
            logger.info(f"     Entities: {', '.join(entities)}")


if __name__ == "__main__":
    main()
