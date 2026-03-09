import json
import yaml
import logging
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, Any, Optional
import traceback

from src.readers.csv_reader import read_csv_in_chunks
from src.validation.schema_validator import validate_schema
from src.profiling.column_profile import profile_columns
from src.reporting.chunk_report import save_chunk_report
from src.reporting.overall_report import save_overall_report
from src.reporting.datadocs_generator import generate_datadocs
from src.utils.secure_logger import SecureNameResolver


class DataQualityPipeline:
    def __init__(self, config_path: str = "configs/dq_config.yaml", schema_path: str="schemas/health_schemas.json"):
        """
        Initialize the DQ Pipeline
        
        Args:
            config_path: Path to configuration YAML file
            schema_path: Path to schema JSON file
        """
        self.config_path = config_path
        self.schema_path = schema_path
        self.logger = self._setup_logging()
        self.config = None
        self.schema = None
        self.overall_stats = defaultdict(int)
        self.total_rows = 0
        self.failed_chunks = []
        self.start_time = None
        
        # Initialize secure name resolver for masking sensitive names in logs
        self.name_resolver = SecureNameResolver("configs/table_mapping.yaml")
        self.column_mask_map = {}  # real_name → masked_name

    def _setup_logging(self) -> logging.Logger:
        # Create logs directory if it doesn't exist
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        # Create logger
        logger = logging.getLogger("DQpipeline")
        logger.setLevel(logging.DEBUG)

        # Prevent duplicate handlers
        if logger.handlers:
            logger.handlers.clear()
        
        # File handler with detailed formatting
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_handler = logging.FileHandler(
            log_dir / f"dq_pipeline_{timestamp}.log",
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        file_handler.setFormatter(file_formatter)

        # Console handler with simpler formatting
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        
        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger
    
    def _load_configuration(self) -> bool:
        """
        Load configuration and schema files with error handling
        
        Returns:
            bool: True if successful, False otherwise
        """

        try:
            self.logger.info("="*80)
            self.logger.info("Loading configuration file")

            # Load config
            config_file = Path(self.config_path)
            if not config_file.exists():
                self.logger.error(f"configuration file not found: {self.config_path}")
                return False
            
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)

            self.logger.info(f"Configuration loaded from: {self.config_path}")
            
            # Log masked config (sensitive paths replaced with dataset IDs)
            masked_config = self.name_resolver.mask_config(self.config)
            self.logger.debug(f"Config contents: {json.dumps(masked_config, indent=2)}")


            # Load schema
            schema_file = Path(self.schema_path)
            if not schema_file.exists():
                self.logger.error(f"Schema file not found")
                return False
            
            with open(schema_file, 'r', encoding='utf-8') as f:
                self.schema = json.load(f)
            
            # Build column masking map from schema
            self.column_mask_map = self.name_resolver.mask_columns_dict(self.schema)
            
            dataset_id = self.name_resolver.get_dataset_id(self.config.get('file_path', ''))
            self.logger.info(f"✓ Schema loaded for dataset: {dataset_id}")
            self.logger.info(f"  Schema contains {len(self.schema)} columns")

            # Validate essential config parameters
            required_params = ['file_path', 'chunk_size']
            missing_params = [p for p in required_params if p not in self.config]
            
            if missing_params:
                self.logger.error(f"Missing required config parameters: {missing_params}")
                return False
            
            # Check if data file exists
            data_file = Path(self.config['file_path'])
            if not data_file.exists():
                self.logger.error(f"Data file not found")
                return False
            
            file_size_mb = data_file.stat().st_size / (1024 * 1024)
            dataset_id = self.name_resolver.get_dataset_id(self.config['file_path'])
            self.logger.info(f"Data file found: {dataset_id} ({file_size_mb:.2f} MB)")
            
            return True
        
        except yaml.YAMLError as e:
            self.logger.error(f"YAML parsing error in config file: {e}")
            self.logger.debug(f"Error type: {type(e).__name__}")
            return False
            
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON parsing error in schema file: {e}")
            self.logger.debug(f"Error type: {type(e).__name__}")
            return False
            
        except Exception as e:
            self.logger.error(f"Unexpected error loading configuration: {e}")
            self.logger.debug(f"Error type: {type(e).__name__}")
            return False
        

    def _create_output_directories(self) -> bool:
        """Create necessary output directories"""
        try:
            directories = [
                Path("outputs/chunk_reports"),
                Path("outputs/summaries"),
                Path("outputs/failed_chunks"),
                Path("outputs/great_expectations")
            ]
            
            for directory in directories:
                directory.mkdir(parents=True, exist_ok=True)
                
            self.logger.info("Output directories created/verified")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create output directories: {e}")
            self.logger.debug(f"Error type: {type(e).__name__}")
            return False
    
    def _clean_output_directories(self) -> bool:
        """Clean output directories before starting new run"""
        try:
            self.logger.info("Cleaning output directories from previous runs...")
        
            # Import shutil for directory deletion
            import shutil
        
            files_deleted = 0
        
            # 1. Clean chunk reports (JSON files)
            chunk_reports_dir = Path("outputs/chunk_reports")
            if chunk_reports_dir.exists():
                json_files = list(chunk_reports_dir.glob("*.json"))
                for file in json_files:
                    file.unlink()
                    files_deleted += 1
                if json_files:
                    self.logger.info(f"  Deleted {len(json_files)} chunk reports")
        
            # 2. Clean failed chunks (JSON files)
            failed_chunks_dir = Path("outputs/failed_chunks")
            if failed_chunks_dir.exists():
                json_files = list(failed_chunks_dir.glob("*.json"))
                for file in json_files:
                    file.unlink()
                    files_deleted += 1
                if json_files:
                   self.logger.info(f"  Deleted {len(json_files)} error reports")
        
            # 3. Clean summaries (all files)
            summaries_dir = Path("outputs/summaries")
            if summaries_dir.exists():
                summary_files = [f for f in summaries_dir.glob("*") if f.is_file()]
                for file in summary_files:
                    file.unlink()
                    files_deleted += 1
                if summary_files:
                    self.logger.info(f"  Deleted {len(summary_files)} summary files")
        
            # 4. Clean Great Expectations (entire directory - can be large!)
            ge_dir = Path("outputs/great_expectations")
            if ge_dir.exists():
                try:
                    shutil.rmtree(ge_dir)
                    self.logger.info(f"  Deleted Great Expectations directory (saves disk space)")
                except Exception as e:
                    self.logger.warning(f"  Could not delete GE directory: {e}")
        
            if files_deleted > 0:
                self.logger.info(f"Cleaned {files_deleted} old files")
            else:
                self.logger.info("No old files to clean")
        
            return True
        except Exception as e:
            self.logger.error(f"Failed to clean directories: {e}")
            self.logger.debug(f"Error type: {type(e).__name__}")
            self.logger.warning("Continuing without cleaning...")
            return True  # Don't fail pipeline
        
    

    def _process_chunk(self, chunk_id: int, chunk) -> Optional[Dict[str, Any]]:
        """
        Process a single chunk with comprehensive error handling
        
        Args:
            chunk_id: Chunk identifier
            chunk: DataFrame chunk to process
            
        Returns:
            Dict with chunk results or None if failed
        """
        try:
            self.logger.info(f"Processing chunk {chunk_id} ({len(chunk)} rows)...")
            
            # Update total row count
            self.total_rows += len(chunk)
            
            # Profile columns
            self.logger.debug(f"Chunk {chunk_id}: Profiling columns...")
            profile = profile_columns(
                chunk,
                include_percentiles=self.config.get("include_percentiles", True),
                include_value_counts=self.config.get("include_value_counts", True),
                max_unique_for_counts=self.config.get("max_unique_for_counts", 50)
            )
            
            # Validate schema
            self.logger.debug(f"Chunk {chunk_id}: Validating schema...")
            validation = validate_schema(
                chunk, self.schema,
                column_masker=self.name_resolver.mask_column
            )
            
            # Count failed expectations
            failed_expectations = [r for r in validation["results"] if not r["success"]]
            
            # Prepare chunk report
            chunk_report = {
                "chunk_id": chunk_id,
                "row_count": len(chunk),
                "validation_success": validation["success"],
                "failed_expectations_count": len(failed_expectations),
                "failed_expectations": failed_expectations if failed_expectations else [],
                "column_profile": profile,
                "processing_timestamp": datetime.now().isoformat()
            }
            
            # Save chunk report (mask column names for security)
            masked_report = self.name_resolver.mask_report_data(chunk_report)
            save_chunk_report(chunk_id, masked_report)
            
            # Track failures
            if not validation["success"]:
                self.overall_stats["failed_chunks"] += 1
                self.failed_chunks.append(chunk_id)
                self.logger.warning(
                    f"Chunk {chunk_id} FAILED validation "
                    f"({len(failed_expectations)} failed expectations)"
                )
            else:
                self.logger.info(f"✓ Chunk {chunk_id} passed validation")
            
            # Update statistics
            self.overall_stats["processed_chunks"] += 1
            self.overall_stats["total_failed_expectations"] += len(failed_expectations)
            
            return chunk_report
            
        except Exception as e:
            self.logger.error(f"Error processing chunk {chunk_id}: {e}")
            self.logger.debug(f"Error type: {type(e).__name__}")
            
            # Track failed chunk
            self.overall_stats["error_chunks"] += 1
            self.failed_chunks.append(chunk_id)
            
            # Save error details
            try:
                error_report = {
                    "chunk_id": chunk_id,
                    "error": str(e),
                    "traceback": "[REDACTED]",
                    "timestamp": datetime.now().isoformat()
                }
                error_path = Path("outputs/failed_chunks") / f"chunk_{chunk_id}_error.json"
                with open(error_path, 'w') as f:
                    json.dump(error_report, f, indent=2)
            except Exception as save_error:
                self.logger.error(f"Failed to save error report for chunk {chunk_id}: {save_error}")
            
            return None
        
    def _generate_overall_report(self) -> bool:
        """Generate and save overall summary report"""
        try:
            elapsed_time = (datetime.now() - self.start_time).total_seconds()
            
            overall_report = {
                "pipeline_metadata": {
                    "config_file": self.config_path,
                    "schema_file": self.name_resolver.get_schema_id(self.config['file_path']),
                    "data_file": self.name_resolver.get_dataset_id(self.config['file_path']),
                    "start_time": self.start_time.isoformat(),
                    "end_time": datetime.now().isoformat(),
                    "elapsed_time_seconds": round(elapsed_time, 2),
                    "chunk_size": self.config['chunk_size']
                },
                "summary_statistics": {
                    "total_rows_processed": self.total_rows,
                    "total_chunks_processed": self.overall_stats["processed_chunks"],
                    "successful_chunks": (
                        self.overall_stats["processed_chunks"] - 
                        self.overall_stats["failed_chunks"]
                    ),
                    "failed_validation_chunks": self.overall_stats["failed_chunks"],
                    "error_chunks": self.overall_stats["error_chunks"],
                    "total_failed_expectations": self.overall_stats["total_failed_expectations"],
                    "success_rate_pct": round(
                        ((self.overall_stats["processed_chunks"] - self.overall_stats["failed_chunks"]) /
                         max(self.overall_stats["processed_chunks"], 1)) * 100, 2
                    )
                },
                "failed_chunk_ids": self.failed_chunks,
                "performance_metrics": {
                    "rows_per_second": round(self.total_rows / max(elapsed_time, 0.001), 2),
                    "chunks_per_second": round(
                        self.overall_stats["processed_chunks"] / max(elapsed_time, 0.001), 2
                    )
                }
            }
            
            save_overall_report(overall_report)
            
            self.logger.info("=" * 80)
            self.logger.info("PIPELINE SUMMARY")
            self.logger.info("=" * 80)
            self.logger.info(f"Total rows processed: {self.total_rows:,}")
            self.logger.info(f"Total chunks processed: {self.overall_stats['processed_chunks']}")
            self.logger.info(f"Successful chunks: {overall_report['summary_statistics']['successful_chunks']}")
            self.logger.info(f"Failed chunks: {self.overall_stats['failed_chunks']}")
            self.logger.info(f"Error chunks: {self.overall_stats['error_chunks']}")
            self.logger.info(f"Success rate: {overall_report['summary_statistics']['success_rate_pct']}%")
            self.logger.info(f"Processing speed: {overall_report['performance_metrics']['rows_per_second']:,.2f} rows/sec")
            self.logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")
            self.logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to generate overall report: {e}")
            self.logger.debug(f"Error type: {type(e).__name__}")
            return False
    
    def run(self) -> bool:
        """
        Execute the complete data quality pipeline
        
        Returns:
            bool: True if pipeline completed successfully, False otherwise
        """
        self.start_time = datetime.now()
        
        try:
            self.logger.info("=" * 80)
            self.logger.info("DATA QUALITY PIPELINE STARTED")
            self.logger.info("=" * 80)
            
            # Load configuration
            if not self._load_configuration():
                self.logger.error("Pipeline failed: Configuration loading error")
                return False
            
            if self.config.get('clean_outputs_before_run', True):
                if not self._clean_output_directories():
                    self.logger.warning("Failed to clean directories, but continuing...")
            
            # Create output directories
            if not self._create_output_directories():
                self.logger.error("Pipeline failed: Directory creation error")
                return False
            
            # Process chunks
            self.logger.info("=" * 80)
            self.logger.info("PROCESSING DATA CHUNKS")
            self.logger.info("=" * 80)
            
            try:
                dataset_id = self.name_resolver.get_dataset_id(self.config["file_path"])
                chunk_iterator = read_csv_in_chunks(
                    self.config["file_path"], 
                    self.config["chunk_size"],
                    dataset_id=dataset_id
                )
                
                for chunk_id, chunk in enumerate(chunk_iterator, start=1):
                    self._process_chunk(chunk_id, chunk)
                    
                    # Log progress every 10 chunks
                    if chunk_id % 10 == 0:
                        self.logger.info(
                            f"Progress: {chunk_id} chunks, "
                            f"{self.total_rows:,} rows processed"
                        )
                
            except Exception as e:
                self.logger.error(f"Error during chunk processing: {e}")
                self.logger.debug(f"Error type: {type(e).__name__}")
                
                # Continue to generate report even if processing failed
                if self.config.get('fail_on_error', False):
                    raise
            
            # Generate overall report
            self._generate_overall_report()
            
            # Generate Great Expectations DataDocs
            if self.config.get('generate_datadocs', True):
                self.logger.info("=" * 80)
                self.logger.info("Generating Great Expectations DataDocs...")
                try:
                    datadocs_path = generate_datadocs(
                        self.config["file_path"],
                        self.schema,
                        self.config["chunk_size"],
                        sample_size=self.config.get("datadocs_sample_size", 10000),
                        dataset_id=dataset_id
                    )
                    self.logger.info(f"✓ DataDocs generated for dataset: {dataset_id}")
                except Exception as e:
                    self.logger.error(f"Failed to generate DataDocs: {e}")
                    self.logger.debug(f"Error type: {type(e).__name__}")
            
            # Final status
            if self.overall_stats["error_chunks"] > 0:
                self.logger.warning(
                    f"Pipeline completed with {self.overall_stats['error_chunks']} error chunks"
                )
            else:
                self.logger.info("✓ Pipeline completed successfully")
            
            return True
            
        except Exception as e:
            self.logger.critical(f"Pipeline failed with critical error: {e}")
            self.logger.debug(f"Error type: {type(e).__name__}")
            return False
        
        finally:
            if self.start_time:
                elapsed = (datetime.now() - self.start_time).total_seconds()
                self.logger.info(f"Pipeline execution time: {elapsed:.2f} seconds")
            self.logger.info("=" * 80)


def main():
    """Main entry point for the pipeline"""
    pipeline = DataQualityPipeline(
        config_path="configs/dq_config.yaml",
        schema_path="schemas/health_schema.json"
    )
    
    success = pipeline.run()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()