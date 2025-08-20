# DataFlow xLerate

## Overview

DataFlow xLerate is a declarative data ingestion framework built in Python that enables users to define data pipelines through YAML configuration files. The framework provides a modular architecture for extracting, transforming, and loading data between various sources and targets including databases (JDBC), local files, and cloud storage. It emphasizes separation of concerns with distinct layers for configuration management, execution orchestration, and data processing operations.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Configuration-Driven Architecture
The system follows a declarative approach where all pipeline logic is defined in YAML configuration files with three hierarchical levels:
- **Global Level**: Project-wide defaults and environment settings
- **Pipeline Level**: Pipeline-specific configurations including platform, orchestrator integration, and engine settings
- **Mapping Level**: Detailed data movement instructions between sources and targets

### Core Components

**CLI Interface**: Entry point that accepts YAML configuration files and provides options for pipeline execution, retry modes, logging levels, and dry-run validation.

**Configuration Management**: 
- YAML parser with environment variable substitution support
- Configuration validator that ensures structural integrity and validates field values
- Hierarchical configuration processing with override capabilities

**Execution Engine**: 
- Pipeline orchestrator that manages mapping execution sequence
- Core engine that handles individual data processing operations
- Support for retry modes (restart vs continue from last successful mapping)

**Store Abstraction**: 
- Factory pattern for creating different store types (JDBC, local filesystem)
- Base store interface defining read/write operations for entities
- JDBC store using SQLAlchemy for database connectivity
- Local file store supporting multiple formats (CSV, Parquet, JSON)

**Transformation Framework**: 
- Factory pattern for creating transformation instances
- Base transformation interface for applying data modifications
- Built-in transformations for schema mapping, filtering, and basic cleanup

**Logging System**: 
- Structured logging with configurable levels
- Console and file output support
- Pipeline execution tracking and error reporting

### Design Patterns
- **Factory Pattern**: Used for creating store and transformation instances based on configuration
- **Strategy Pattern**: Different store implementations for various data sources
- **Template Method**: Base classes define common behavior while allowing specific implementations

### Error Handling and Resilience
- Comprehensive validation at configuration and runtime levels
- Retry mechanisms with different modes (restart entire pipeline vs continue from failure point)
- Graceful error handling with detailed logging and status reporting

## External Dependencies

**Core Libraries**:
- `pandas`: Primary data manipulation and analysis library
- `sqlalchemy`: Database connectivity and ORM functionality
- `pyyaml`: YAML configuration file parsing
- `click`: Command-line interface framework
- `pathlib`: Cross-platform path handling

**Database Support**:
- SQLAlchemy-compatible databases through JDBC connections
- PostgreSQL environment variable support (PGHOST, etc.)
- Connection pooling and health checking

**File System Support**:
- Local file system operations
- Support for CSV, Parquet, and JSON formats
- Path templating with entity substitution

**Environment Integration**:
- Environment variable substitution in configurations
- Configurable logging levels and output destinations
- Integration hooks for external orchestrators like Airflow