"""
JDBC store implementation for database connectivity
"""

import pandas as pd
import os
from typing import Dict, Any, Optional
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from .base import BaseStore


class JDBCStore(BaseStore):
    """JDBC store for database connectivity using SQLAlchemy"""
    
    def __init__(self, config: Dict[str, Any], logger):
        super().__init__(config, logger)
        self.engine = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize database connection"""
        try:
            connection_url = self._build_connection_url()
            self.logger.info(f"Connecting to database: {self._get_safe_connection_info()}")
            
            self.engine = create_engine(
                connection_url,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.logger.info("Database connection established successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to establish database connection: {str(e)}")
            raise
    
    def _build_connection_url(self) -> str:
        """Build SQLAlchemy connection URL"""
        # Check if connection_url is directly provided
        if "connection_url" in self.store_config:
            return self.store_config["connection_url"]
        
        # Check for environment variables (PostgreSQL defaults)
        pg_host = os.getenv("PGHOST")
        pg_port = os.getenv("PGPORT", "5432")
        pg_database = os.getenv("PGDATABASE")
        pg_user = os.getenv("PGUSER")
        pg_password = os.getenv("PGPASSWORD")
        
        if pg_host and pg_database and pg_user and pg_password:
            return f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
        
        # Check for generic DATABASE_URL
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            return database_url
        
        # Build from individual config parameters
        db_type = self.store_config.get("db_type", "postgresql")
        host = self.store_config.get("host", "localhost")
        port = self.store_config.get("port", 5432 if db_type == "postgresql" else 3306)
        database = self.store_config.get("db_name", self.store_config.get("database"))
        username = self.store_config.get("username", self.store_config.get("user"))
        password = self.store_config.get("password")
        
        if not all([database, username, password]):
            raise ValueError("Insufficient database connection parameters")
        
        if db_type in ["postgresql", "postgres"]:
            return f"postgresql://{username}:{password}@{host}:{port}/{database}"
        elif db_type == "mysql":
            return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def _get_safe_connection_info(self) -> str:
        """Get connection info for logging (without password)"""
        if "connection_url" in self.store_config:
            url = self.store_config["connection_url"]
            # Mask password in URL
            if "://" in url and "@" in url:
                protocol, rest = url.split("://", 1)
                if "@" in rest:
                    credentials, host_part = rest.split("@", 1)
                    if ":" in credentials:
                        user, _ = credentials.split(":", 1)
                        return f"{protocol}://{user}:***@{host_part}"
            return url
        
        db_type = self.store_config.get("db_type", "postgresql")
        host = self.store_config.get("host", "localhost")
        port = self.store_config.get("port", 5432 if db_type == "postgresql" else 3306)
        database = self.store_config.get("db_name", self.store_config.get("database"))
        
        return f"{db_type}://{host}:{port}/{database}"
    
    def read_entity(self, entity: str) -> Optional[pd.DataFrame]:
        """Read data from a database table"""
        try:
            self.logger.info(f"Reading data from table: {entity}")
            
            # Build query
            query = f"SELECT * FROM {entity}"
            
            # Execute query and return DataFrame
            df = pd.read_sql(query, self.engine)
            
            self.logger.info(f"Successfully read {len(df)} rows from table: {entity}")
            return df
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error reading table {entity}: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Error reading table {entity}: {str(e)}")
            return None
    
    def write_entity(self, entity: str, data: pd.DataFrame, write_mode: str = "overwrite") -> bool:
        """Write data to a database table"""
        try:
            self.logger.info(f"Writing {len(data)} rows to table: {entity} (mode: {write_mode})")
            
            # Map write modes to pandas to_sql parameters
            if write_mode == "overwrite":
                if_exists = "replace"
            elif write_mode == "append":
                if_exists = "append"
            elif write_mode in ["upsert", "upsert_only"]:
                # For MVP, treat upsert as replace
                # Full implementation would handle proper upsert logic
                self.logger.warning(f"Upsert mode not fully implemented, using replace for table: {entity}")
                if_exists = "replace"
            else:
                self.logger.error(f"Unsupported write mode: {write_mode}")
                return False
            
            # Write to database
            data.to_sql(
                entity,
                self.engine,
                if_exists=if_exists,
                index=False,
                method='multi'
            )
            
            self.logger.info(f"Successfully wrote data to table: {entity}")
            return True
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error writing to table {entity}: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error writing to table {entity}: {str(e)}")
            return False
    
    def list_entities(self) -> list:
        """List all tables in the database"""
        try:
            if self.engine is None:
                self.logger.error("Database engine not initialized")
                return []
            inspector = inspect(self.engine)
            if inspector is None:
                self.logger.error("Database inspector not available")
                return []
            tables = inspector.get_table_names()
            self.logger.info(f"Found {len(tables)} tables in database")
            return tables
            
        except Exception as e:
            self.logger.error(f"Error listing tables: {str(e)}")
            return []
    
    def entity_exists(self, entity: str) -> bool:
        """Check if a table exists in the database"""
        try:
            if self.engine is None:
                self.logger.error("Database engine not initialized")
                return False
            inspector = inspect(self.engine)
            if inspector is None:
                self.logger.error("Database inspector not available")
                return False
            return entity in inspector.get_table_names()
            
        except Exception as e:
            self.logger.error(f"Error checking if table {entity} exists: {str(e)}")
            return False
    
    def get_connection_info(self) -> str:
        """Get connection info for logging"""
        return f"JDBC store: {self._get_safe_connection_info()}"
    
    def close(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database connection closed")
