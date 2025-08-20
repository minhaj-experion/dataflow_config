"""
Basic transformation implementations
"""

import pandas as pd
from typing import Dict, Any, List
from .base import BaseTransformation


class SchemaMapTransformation(BaseTransformation):
    """Schema mapping transformation for column renaming and selection"""
    
    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply schema mapping transformation"""
        try:
            self.logger.info("Applying schema mapping transformation")
            
            # Get mapping configuration
            column_mapping = self.config.get("column_mapping", {})
            selected_columns = self.config.get("selected_columns", [])
            
            result_data = data.copy()
            
            # Apply column renaming
            if column_mapping:
                result_data = result_data.rename(columns=column_mapping)
                self.logger.info(f"Renamed columns: {column_mapping}")
            
            # Select specific columns if specified
            if selected_columns:
                # Filter to only include columns that exist
                available_columns = [col for col in selected_columns if col in result_data.columns]
                if available_columns:
                    result_data = result_data[available_columns]
                    self.logger.info(f"Selected columns: {available_columns}")
                else:
                    self.logger.warning("No specified columns found in data")
            
            self.logger.info(f"Schema mapping completed. Columns: {list(result_data.columns)}")
            return result_data
            
        except Exception as e:
            self.logger.error(f"Error in schema mapping transformation: {str(e)}")
            return data


class FilterTransformation(BaseTransformation):
    """Data filtering transformation"""
    
    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply filtering transformation"""
        try:
            self.logger.info("Applying filter transformation")
            
            conditions = self.config.get("conditions", [])
            result_data = data.copy()
            
            original_count = len(result_data)
            
            for condition in conditions:
                result_data = self._apply_condition(result_data, condition)
            
            filtered_count = len(result_data)
            self.logger.info(f"Filter transformation completed. Rows: {original_count} -> {filtered_count}")
            
            return result_data
            
        except Exception as e:
            self.logger.error(f"Error in filter transformation: {str(e)}")
            return data
    
    def _apply_condition(self, data: pd.DataFrame, condition: Dict[str, Any]) -> pd.DataFrame:
        """Apply a single filter condition"""
        column = condition.get("column")
        operator = condition.get("operator")
        value = condition.get("value")
        
        if not all([column, operator]):
            self.logger.warning(f"Invalid condition: {condition}")
            return data
        
        if column not in data.columns:
            self.logger.warning(f"Column {column} not found in data")
            return data
        
        try:
            if operator == "equals":
                return data[data[column] == value]
            elif operator == "not_equals":
                return data[data[column] != value]
            elif operator == "greater_than":
                return data[data[column] > value]
            elif operator == "less_than":
                return data[data[column] < value]
            elif operator == "greater_equal":
                return data[data[column] >= value]
            elif operator == "less_equal":
                return data[data[column] <= value]
            elif operator == "in":
                return data[data[column].isin(value)]
            elif operator == "not_in":
                return data[~data[column].isin(value)]
            elif operator == "is_null":
                return data[data[column].isna()]
            elif operator == "is_not_null":
                return data[data[column].notna()]
            else:
                self.logger.warning(f"Unsupported operator: {operator}")
                return data
                
        except Exception as e:
            self.logger.error(f"Error applying condition {condition}: {str(e)}")
            return data


class BasicCleanupTransformation(BaseTransformation):
    """Basic data cleanup transformation"""
    
    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply basic cleanup transformation"""
        try:
            self.logger.info("Applying basic cleanup transformation")
            
            result_data = data.copy()
            original_count = len(result_data)
            
            # Remove empty rows
            if self.config.get("remove_empty_rows", True):
                result_data = result_data.dropna(how='all')
            
            # Remove duplicate rows
            if self.config.get("remove_duplicates", False):
                result_data = result_data.drop_duplicates()
            
            # Trim whitespace from string columns
            if self.config.get("trim_whitespace", True):
                string_columns = result_data.select_dtypes(include=['object']).columns
                result_data[string_columns] = result_data[string_columns].apply(
                    lambda x: x.str.strip() if x.dtype == 'object' else x
                )
            
            # Clean column names
            if self.config.get("clean_column_names", True):
                result_data.columns = result_data.columns.str.strip().str.replace(' ', '_')
            
            final_count = len(result_data)
            self.logger.info(f"Basic cleanup completed. Rows: {original_count} -> {final_count}")
            
            return result_data
            
        except Exception as e:
            self.logger.error(f"Error in basic cleanup transformation: {str(e)}")
            return data
