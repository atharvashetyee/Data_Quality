"""
Enhanced Column Profiling with comprehensive statistics and error handling
"""
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Optional
from collections import Counter

logger = logging.getLogger("DQPipeline.column_profile")


def to_python(val):
    """Convert pandas/numpy types to native Python types"""
    if pd.isna(val):
        return None
    if hasattr(val, "item"):
        return val.item()
    return val


def profile_columns(
    df: pd.DataFrame,
    include_percentiles: bool = True,
    include_value_counts: bool = True,
    max_unique_for_counts: int = 50
) -> Dict[str, Dict[str, Any]]:
    """
    Generate comprehensive column profiles 
    
    Args:
        df: DataFrame to profile
        include_percentiles: Include 25th, 50th, 75th percentiles
        include_value_counts: Include top value counts
        max_unique_for_counts: Max unique values to include counts for
        
    Returns:
        Dictionary of column profiles
    """
    profile = {}
    
    logger.debug(f"Profiling {len(df.columns)} columns for {len(df)} rows")
    
    for col in df.columns:
        try:
            stats = _profile_single_column(
                df[col], 
                col,
                include_percentiles,
                include_value_counts,
                max_unique_for_counts
            )
            profile[col] = stats
            
        except Exception as e:
            logger.error(f"Error profiling column '{col}': {e}")
            # Add minimal profile on error
            profile[col] = {
                "dtype": str(df[col].dtype),
                "error": str(e)
            }
    
    logger.debug(f"Successfully profiled {len(profile)} columns")
    return profile


def _profile_single_column(
    series: pd.Series,
    col_name: str,
    include_percentiles: bool,
    include_value_counts: bool,
    max_unique_for_counts: int
) -> Dict[str, Any]:
    """Profile a single column with comprehensive statistics"""
    
    stats = {
        "dtype": str(series.dtype),
        "null_count": int(series.isnull().sum()),
        "null_pct": float(series.isnull().mean() * 100),  # Convert to percentage
        "non_null_count": int(series.notna().sum()),
        "distinct_count": int(series.nunique()),
        "distinct_pct": float((series.nunique() / len(series)) * 100) if len(series) > 0 else 0
    }
    
    # Numeric statistics
    if pd.api.types.is_numeric_dtype(series):
        try:
            stats.update(_get_numeric_stats(series, include_percentiles))
        except Exception as e:
            logger.warning(f"Error calculating numeric stats for '{col_name}': {e}")
    
    # String/Object statistics
    elif pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
        try:
            stats.update(_get_string_stats(series))
        except Exception as e:
            logger.warning(f"Error calculating string stats for '{col_name}': {e}")
    
    # Datetime statistics
    elif pd.api.types.is_datetime64_any_dtype(series):
        try:
            stats.update(_get_datetime_stats(series))
        except Exception as e:
            logger.warning(f"Error calculating datetime stats for '{col_name}': {e}")
    
    # Value counts for low cardinality columns
    if include_value_counts and stats["distinct_count"] <= max_unique_for_counts:
        try:
            value_counts = series.value_counts().head(20)
            stats["top_values"] = {
                str(k): int(v) for k, v in value_counts.items()
            }
        except Exception as e:
            logger.warning(f"Error calculating value counts for '{col_name}': {e}")
    
    # Convert numpy types to Python types for JSON serialization
    stats = {k: to_python(v) if not isinstance(v, dict) else v 
             for k, v in stats.items()}
    
    return stats


def _get_numeric_stats(series: pd.Series, include_percentiles: bool) -> Dict[str, Any]:
    """Calculate statistics for numeric columns"""
    stats = {
        "min": to_python(series.min()),
        "max": to_python(series.max()),
        "mean": to_python(series.mean()),
        "median": to_python(series.median()),
        "std": to_python(series.std()),
        "sum": to_python(series.sum()),
        "variance": to_python(series.var())
    }
    
    if include_percentiles:
        stats.update({
            "percentile_25": to_python(series.quantile(0.25)),
            "percentile_50": to_python(series.quantile(0.50)),
            "percentile_75": to_python(series.quantile(0.75))
        })
    
    # Check for zeros
    zero_count = int((series == 0).sum())
    if zero_count > 0:
        stats["zero_count"] = zero_count
        stats["zero_pct"] = float((zero_count / len(series)) * 100)
    
    # Check for negative values
    negative_count = int((series < 0).sum())
    if negative_count > 0:
        stats["negative_count"] = negative_count
        stats["negative_pct"] = float((negative_count / len(series)) * 100)
    
    return stats


def _get_string_stats(series: pd.Series) -> Dict[str, Any]:
    """Calculate statistics for string/object columns"""
    # Remove null values for string analysis
    valid_series = series.dropna()
    
    if len(valid_series) == 0:
        return {}
    
    # Convert to string if needed
    str_series = valid_series.astype(str)
    
    stats = {
        "min_length": int(str_series.str.len().min()),
        "max_length": int(str_series.str.len().max()),
        "avg_length": float(str_series.str.len().mean()),
        "empty_string_count": int((str_series == "").sum())
    }
    
    # Check for common patterns
    whitespace_only = int(str_series.str.strip().eq("").sum())
    if whitespace_only > 0:
        stats["whitespace_only_count"] = whitespace_only
    
    return stats


def _get_datetime_stats(series: pd.Series) -> Dict[str, Any]:
    """Calculate statistics for datetime columns"""
    valid_series = series.dropna()
    
    if len(valid_series) == 0:
        return {}
    
    stats = {
        "min_date": str(valid_series.min()),
        "max_date": str(valid_series.max()),
        "date_range_days": int((valid_series.max() - valid_series.min()).days)
    }
    
    return stats


def profile_dataframe_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate a high-level summary profile of the entire DataFrame
    
    Args:
        df: DataFrame to summarize
        
    Returns:
        Summary statistics dictionary
    """
    try:
        summary = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "memory_usage_mb": float(df.memory_usage(deep=True).sum() / (1024 * 1024)),
            "duplicate_rows": int(df.duplicated().sum()),
            "duplicate_rows_pct": float((df.duplicated().sum() / len(df)) * 100) if len(df) > 0 else 0,
            "columns": list(df.columns),
            "dtypes": df.dtypes.astype(str).to_dict()
        }
        
        # Calculate completeness
        total_cells = len(df) * len(df.columns)
        null_cells = df.isnull().sum().sum()
        summary["total_cells"] = total_cells
        summary["null_cells"] = int(null_cells)
        summary["completeness_pct"] = float(((total_cells - null_cells) / total_cells) * 100) if total_cells > 0 else 0
        
        return summary
        
    except Exception as e:
        logger.error(f"Error creating DataFrame summary: {e}")
        return {"error": str(e)}