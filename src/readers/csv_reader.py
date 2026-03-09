"""
Enhanced CSV Reader with parallel processing and memory optimization
Optimized for large datasets (50M+ rows, 1000 columns)
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Iterator, Optional, List
import multiprocessing as mp
from functools import partial

logger = logging.getLogger("DQPipeline.csv_reader")


def read_csv_in_chunks(
    file_path: str, 
    chunk_size: int,
    usecols: Optional[List[str]] = None,
    dtype: Optional[dict] = None,
    low_memory: bool = True,
    dataset_id: Optional[str] = None
) -> Iterator[pd.DataFrame]:
    """
    Read CSV file in chunks with optimized memory usage
    
    Args:
        file_path: Path to CSV file
        chunk_size: Number of rows per chunk
        usecols: Optional list of columns to read (reduces memory for wide datasets)
        dtype: Optional dictionary of column data types (improves performance)
        low_memory: Use low memory mode (default True)
        
    Yields:
        DataFrame chunks
        
    Raises:
        FileNotFoundError: If file doesn't exist
        pd.errors.ParserError: If CSV parsing fails
    """
    try:
        file_path_obj = Path(file_path)
        
        if not file_path_obj.exists():
            raise FileNotFoundError(f"CSV file not found: {dataset_id or '[MASKED]'}")
        
        logger.info(f"Starting to read CSV file: {dataset_id or '[MASKED]'}")
        logger.info(f"Chunk size: {chunk_size:,} rows")
        
        # Read in chunks with optimizations
        chunk_iter = pd.read_csv(
            file_path,
            chunksize=chunk_size,
            usecols=usecols,
            dtype=dtype,
            low_memory=low_memory,
            # Additional optimizations for large files
            engine='c',  # Use faster C parser
            na_filter=True,  # Detect missing values
            keep_default_na=True
        )
        
        chunks_read = 0
        for chunk in chunk_iter:
            chunks_read += 1
            
            # Log progress for every 100 chunks
            if chunks_read % 100 == 0:
                logger.debug(f"Read {chunks_read} chunks ({chunks_read * chunk_size:,} rows)")
            
            yield chunk
        
        logger.info(f"Completed reading {chunks_read} chunks from CSV file")
        
    except FileNotFoundError:
        logger.error(f"File not found: {dataset_id or '[MASKED]'}")
        raise
        
    except pd.errors.ParserError as e:
        logger.error(f"CSV parsing error: {e}")
        raise
        
    except Exception as e:
        logger.error(f"Unexpected error reading CSV: {e}")
        raise


def read_csv_optimized(
    file_path: str,
    chunk_size: int,
    schema: Optional[dict] = None
) -> Iterator[pd.DataFrame]:
    """
    Read CSV with schema-based optimizations
    
    Args:
        file_path: Path to CSV file
        chunk_size: Number of rows per chunk
        schema: Optional schema dictionary to infer dtypes
        
    Yields:
        Optimized DataFrame chunks
    """
    # Build dtype dictionary from schema if provided
    dtype_dict = None
    if schema:
        dtype_dict = {}
        for col, rules in schema.items():
            col_dtype = rules.get('dtype', 'object')
            # Map Great Expectations types to pandas types
            if col_dtype == 'int64':
                dtype_dict[col] = 'Int64'  # Nullable integer
            elif col_dtype == 'float64':
                dtype_dict[col] = 'float64'
            elif col_dtype == 'object':
                dtype_dict[col] = 'string'  # More memory efficient than object
        
        logger.info(f"Using schema-optimized dtypes for {len(dtype_dict)} columns")
    
    return read_csv_in_chunks(
        file_path=file_path,
        chunk_size=chunk_size,
        dtype=dtype_dict,
        low_memory=True
    )


def estimate_optimal_chunk_size(
    file_path: str,
    available_memory_gb: float = 4.0,
    sample_rows: int = 10000
) -> int:
    """
    Estimate optimal chunk size based on available memory and data characteristics
    
    Args:
        file_path: Path to CSV file
        available_memory_gb: Available RAM in GB
        sample_rows: Number of rows to sample for estimation
        
    Returns:
        Recommended chunk size
    """
    try:
        logger.info("Estimating optimal chunk size...")
        
        # Read a sample
        sample_df = pd.read_csv(file_path, nrows=sample_rows)
        
        # Calculate memory per row (in bytes)
        memory_per_row = sample_df.memory_usage(deep=True).sum() / len(sample_df)
        
        # Use 80% of available memory for safety
        usable_memory_bytes = available_memory_gb * 0.8 * 1024 * 1024 * 1024
        
        # Calculate chunk size
        recommended_chunk_size = int(usable_memory_bytes / memory_per_row)
        
        # Round to nearest 1000
        recommended_chunk_size = (recommended_chunk_size // 1000) * 1000
        
        # Ensure reasonable bounds
        recommended_chunk_size = max(1000, min(recommended_chunk_size, 1000000))
        
        logger.info(f"Memory per row: {memory_per_row:.2f} bytes")
        logger.info(f"Recommended chunk size: {recommended_chunk_size:,} rows")
        
        return recommended_chunk_size
        
    except Exception as e:
        logger.error(f"Failed to estimate chunk size: {e}")
        logger.warning("Using default chunk size of 10,000 rows")
        return 10000


def parallel_csv_processor(
    file_path: str,
    chunk_size: int,
    process_func: callable,
    num_workers: Optional[int] = None
) -> List:
    """
    Process CSV chunks in parallel using multiprocessing
    
    Args:
        file_path: Path to CSV file
        chunk_size: Number of rows per chunk
        process_func: Function to apply to each chunk
        num_workers: Number of parallel workers (default: CPU count - 1)
        
    Returns:
        List of processing results
    """
    if num_workers is None:
        num_workers = max(1, mp.cpu_count() - 1)
    
    logger.info(f"Using {num_workers} parallel workers for processing")
    
    results = []
    
    try:
        with mp.Pool(processes=num_workers) as pool:
            # Create chunk iterator
            chunks = list(read_csv_in_chunks(file_path, chunk_size))
            
            # Process in parallel
            results = pool.map(process_func, chunks)
        
        logger.info(f"Completed parallel processing of {len(results)} chunks")
        return results
        
    except Exception as e:
        logger.error(f"Error in parallel processing: {e}")
        raise