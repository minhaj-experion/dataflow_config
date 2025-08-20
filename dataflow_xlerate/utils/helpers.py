"""
Helper utility functions
"""

import os
import re
from pathlib import Path
from typing import Any, Dict, Union, Optional


def substitute_variables(obj: Any) -> Any:
    """
    Recursively substitute environment variables and dynamic references in configuration
    
    Supports:
    - Environment variables: ${ENV_VAR}
    - Entity references: {entity}, ${entity}$
    
    Args:
        obj: Configuration object (dict, list, string, etc.)
        
    Returns:
        Object with variables substituted
    """
    if isinstance(obj, dict):
        return {key: substitute_variables(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [substitute_variables(item) for item in obj]
    elif isinstance(obj, str):
        return _substitute_string_variables(obj)
    else:
        return obj


def _substitute_string_variables(text: str) -> str:
    """Substitute variables in a string"""
    # Environment variable substitution: ${VAR_NAME}
    env_pattern = r'\$\{([^}]+)\}'
    
    def replace_env_var(match):
        var_name = match.group(1)
        return os.getenv(var_name, match.group(0))  # Return original if not found
    
    text = re.sub(env_pattern, replace_env_var, text)
    
    # Note: Entity substitution (${entity}$, {entity}) is left as-is for runtime substitution
    # This will be handled by the store implementations when they know the actual entity name
    
    return text


def validate_path(path: Union[str, Path], must_exist: bool = True, create_if_missing: bool = False) -> Path:
    """
    Validate and optionally create a path
    
    Args:
        path: Path to validate
        must_exist: Whether the path must already exist
        create_if_missing: Whether to create the path if it doesn't exist
        
    Returns:
        Validated Path object
        
    Raises:
        ValueError: If path validation fails
    """
    path_obj = Path(path)
    
    if must_exist and not path_obj.exists():
        if create_if_missing:
            try:
                path_obj.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                raise ValueError(f"Cannot create path {path}: {e}")
        else:
            raise ValueError(f"Path does not exist: {path}")
    
    return path_obj


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string
    """
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.1f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.1f}s"


def safe_get_nested(data: Dict[str, Any], keys: str, default: Any = None) -> Any:
    """
    Safely get nested dictionary value using dot notation
    
    Args:
        data: Dictionary to search
        keys: Dot-separated key path (e.g., "level1.level2.key")
        default: Default value if key not found
        
    Returns:
        Value at the key path or default
    """
    try:
        result = data
        for key in keys.split('.'):
            result = result[key]
        return result
    except (KeyError, TypeError):
        return default


def deep_merge_dicts(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two dictionaries, with dict2 values taking precedence
    
    Args:
        dict1: Base dictionary
        dict2: Override dictionary
        
    Returns:
        Merged dictionary
    """
    result = dict1.copy()
    
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    
    return result


def mask_sensitive_data(data: str, patterns: Optional[list] = None) -> str:
    """
    Mask sensitive data in strings (passwords, API keys, etc.)
    
    Args:
        data: String that may contain sensitive data
        patterns: List of regex patterns to mask (default: common sensitive patterns)
        
    Returns:
        String with sensitive data masked
    """
    if patterns is None:
        patterns = [
            r'password["\']?\s*[:=]\s*["\']?([^"\'\s]+)',
            r'api_key["\']?\s*[:=]\s*["\']?([^"\'\s]+)',
            r'secret["\']?\s*[:=]\s*["\']?([^"\'\s]+)',
            r'token["\']?\s*[:=]\s*["\']?([^"\'\s]+)',
        ]
    
    masked_data = data
    for pattern in patterns:
        masked_data = re.sub(pattern, lambda m: m.group(0).replace(m.group(1), '***'), masked_data, flags=re.IGNORECASE)
    
    return masked_data
