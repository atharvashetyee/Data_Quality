"""
Great Expectations DataDocs Generator
Generates comprehensive HTML reports for data quality validation
"""
import great_expectations as ge
from great_expectations.data_context import BaseDataContext
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core import ExpectationConfiguration
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import json
import traceback

logger = logging.getLogger("DQPipeline.datadocs_generator")


def generate_datadocs(
    file_path: str,
    schema: Dict[str, Any],
    chunk_size: int = 10000,
    sample_size: Optional[int] = None,
    dataset_id: Optional[str] = None
) -> str:
    """
    Generate Great Expectations DataDocs for the dataset
    
    Args:
        file_path: Path to CSV file
        schema: Schema dictionary with validation rules
        chunk_size: Chunk size for reading data
        sample_size: Optional sample size (if None, uses full dataset)
        
    Returns:
        Path to generated DataDocs
    """
    try:
        logger.info("Initializing Great Expectations DataDocs generation...")
        
        # Create output directory
        ge_root = Path("outputs/great_expectations").resolve()
        ge_root.mkdir(parents=True, exist_ok=True)
        
        # Initialize data context
        logger.info("Creating Great Expectations data context...")
        context = _create_data_context(ge_root)
        
        # Load data (sample if specified)
        logger.info(f"Loading data for dataset: {dataset_id or '[MASKED]'}")
        if sample_size:
            df = pd.read_csv(file_path, nrows=sample_size)
            logger.info(f"Loaded {len(df)} rows (sampled from full dataset)")
        else:
            # For large files, read in chunks and validate progressively
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} rows")
        
        # Create expectation suite from schema
        suite_name = "health_data_validation_suite"
        logger.info(f"Creating expectation suite: {suite_name}")
        suite = _create_expectation_suite_from_schema(context, schema, suite_name)
        
        # CREATE AND RUN CHECKPOINT (saves results automatically)
        
        from great_expectations.checkpoint import SimpleCheckpoint
        from datetime import datetime
        
        # Create checkpoint configuration
        checkpoint_name = "health_data_checkpoint"
        checkpoint_config = {
            "name": checkpoint_name,
            "config_version": 1.0,
            "class_name": "SimpleCheckpoint",
            "run_name_template": f"%Y%m%d-%H%M%S-health-data-validation",
        }
        
        # Add checkpoint
        try:
            context.add_checkpoint(**checkpoint_config)
            logger.info(f"Created checkpoint: {checkpoint_name}")
        except Exception as e:
            logger.debug(f"Checkpoint may already exist: {e}")
        
        # Run checkpoint (validates and saves results)
        logger.info("Running validation checkpoint...")
        
        checkpoint_result = context.run_checkpoint(
            checkpoint_name=checkpoint_name,
            validations=[
                {
                    "batch_request": {
                        "datasource_name": "pandas_datasource",
                        "data_connector_name": "runtime_data_connector",
                        "data_asset_name": "health_data",
                        "runtime_parameters": {"batch_data": df},
                        "batch_identifiers": {"default_identifier_name": "default_identifier"}
                    },
                    "expectation_suite_name": suite_name
                }
            ]
        )
        
        # Get results
        results = list(checkpoint_result.run_results.values())[0]["validation_result"]
        
        # Log results summary
        success_count = results.statistics.get("successful_expectations", 0)
        total_count = results.statistics.get("evaluated_expectations", 0)
        success_pct = (success_count / total_count * 100) if total_count > 0 else 0
        
        logger.info(
            f"Validation complete: {success_count}/{total_count} expectations passed "
            f"({success_pct:.1f}%)"
        )
        
        
        # Build Data Docs
        logger.info("Building Data Docs...")
        context.build_data_docs()
        
        # Get Data Docs path
        docs_path = ge_root / "uncommitted" / "data_docs" / "local_site" / "index.html"
        
        if docs_path.exists():
            logger.info(f"✓ Data Docs generated successfully: {docs_path}")
            logger.info(f"  Open in browser: file://{docs_path.absolute()}")
            return str(docs_path.absolute())
        else:
            logger.warning("Data Docs generation completed but index.html not found")
            return str(ge_root)
            
    except Exception as e:
        logger.error(f"Failed to generate DataDocs: {e}")
        logger.debug(f"Error type: {type(e).__name__}")
        raise


def _create_data_context(ge_root: Path) -> BaseDataContext:
    """Create and configure Great Expectations data context"""
    from great_expectations.data_context import FileDataContext
    from great_expectations.data_context.types.base import (
        DataContextConfig,
        DatasourceConfig,
        FilesystemStoreBackendDefaults
    )
    
    # Create GE directory structure
    (ge_root / "expectations").mkdir(exist_ok=True)
    (ge_root / "checkpoints").mkdir(exist_ok=True)
    (ge_root / "plugins").mkdir(exist_ok=True)
    (ge_root / "uncommitted").mkdir(exist_ok=True)
    (ge_root / "uncommitted" / "data_docs").mkdir(exist_ok=True)
    (ge_root / "uncommitted" / "validations").mkdir(exist_ok=True)
    
    # Create data context configuration
    data_context_config = DataContextConfig(
        store_backend_defaults=FilesystemStoreBackendDefaults(
            root_directory=str(ge_root)
        ),
        datasources={
            "pandas_datasource": DatasourceConfig(
                class_name="Datasource",
                execution_engine={
                    "class_name": "PandasExecutionEngine"
                },
                data_connectors={
                    "runtime_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
                    }
                }
            )
        }
    )
    
    # Create and return context
    context = FileDataContext(
        project_config=data_context_config,
        context_root_dir=str(ge_root)
    )
    
    return context


def _create_expectation_suite_from_schema(
    context: BaseDataContext,
    schema: Dict[str, Any],
    suite_name: str
) -> Any:
    """Create expectation suite from schema definition"""
    from great_expectations.core import ExpectationConfiguration
    
    # Create or get suite
    try:
        suite = context.get_expectation_suite(suite_name)
        logger.info(f"Retrieved existing expectation suite: {suite_name}")
    except:
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)
        logger.info(f"Created new expectation suite: {suite_name}")
    
    # Add expectations from schema
    expectations_added = 0
    
    for col, rules in schema.items():
        logger.debug(f"Adding expectations for column: {col}")
        
        try:
            # 1. Column exists
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_to_exist",
                    kwargs={"column": col}
                )
            )
            expectations_added += 1
            
            # 2. Data type
            if "dtype" in rules:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_of_type",
                        kwargs={
                            "column": col,
                            "type_": rules["dtype"]
                        }
                    )
                )
                expectations_added += 1
            
            # 3. Null percentage
            if "nullable_pct" in rules:
                mostly_threshold = 1 - (rules["nullable_pct"] / 100)
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_not_be_null",
                        kwargs={
                            "column": col,
                            "mostly": mostly_threshold
                        }
                    )
                )
                expectations_added += 1
            
            # 4. Value range
            if "min" in rules or "max" in rules:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_between",
                        kwargs={
                            "column": col,
                            "min_value": rules.get("min"),
                            "max_value": rules.get("max")
                        }
                    )
                )
                expectations_added += 1
            
            # 5. Uniqueness
            if rules.get("unique", False):
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_unique",
                        kwargs={"column": col}
                    )
                )
                expectations_added += 1
            
            # 6. String length
            if "min_length" in rules or "max_length" in rules:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_value_lengths_to_be_between",
                        kwargs={
                            "column": col,
                            "min_value": rules.get("min_length"),
                            "max_value": rules.get("max_length")
                        }
                    )
                )
                expectations_added += 1
            
            # 7. Set membership
            if "value_set" in rules:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_in_set",
                        kwargs={
                            "column": col,
                            "value_set": rules["value_set"]
                        }
                    )
                )
                expectations_added += 1
            
            # 8. Regex pattern
            if "regex_pattern" in rules:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_match_regex",
                        kwargs={
                            "column": col,
                            "regex": rules["regex_pattern"]
                        }
                    )
                )
                expectations_added += 1
        
        except Exception as e:
            logger.warning(f"Failed to add expectation for column '{col}': {e}")
            continue
    
    # Save the suite
    try:
        context.save_expectation_suite(expectation_suite=suite)
        logger.info(f"✓ Added {expectations_added} expectations from schema")
    except Exception as e:
        logger.warning(f"Failed to save expectation suite: {e}")
    
    return suite


def generate_simple_html_report(
    validation_results: Dict[str, Any],
    output_path: str = "outputs/summaries/validation_report.html"
) -> str:
    """
    Generate a simple HTML report from validation results
    
    Args:
        validation_results: Great Expectations validation results
        output_path: Path to save HTML report
        
    Returns:
        Path to generated report
    """
    try:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Extract statistics
        stats = validation_results.get("statistics", {})
        results = validation_results.get("results", [])
        
        success_count = stats.get("successful_expectations", 0)
        total_count = stats.get("evaluated_expectations", 0)
        success_pct = (success_count / total_count * 100) if total_count > 0 else 0
        
        # Generate HTML
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Quality Validation Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: 0 auto; background-color: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                h1 {{ color: #333; border-bottom: 3px solid #4CAF50; padding-bottom: 10px; }}
                h2 {{ color: #555; margin-top: 30px; }}
                .summary {{ background-color: #e8f5e9; padding: 20px; border-radius: 5px; margin: 20px 0; }}
                .success {{ color: #4CAF50; font-weight: bold; font-size: 24px; }}
                .failed {{ color: #f44336; font-weight: bold; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th {{ background-color: #4CAF50; color: white; padding: 12px; text-align: left; }}
                td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
                tr:hover {{ background-color: #f5f5f5; }}
                .status-pass {{ color: #4CAF50; font-weight: bold; }}
                .status-fail {{ color: #f44336; font-weight: bold; }}
                .progress-bar {{ width: 100%; height: 30px; background-color: #f0f0f0; border-radius: 15px; overflow: hidden; }}
                .progress-fill {{ height: 100%; background-color: #4CAF50; text-align: center; line-height: 30px; color: white; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Data Quality Validation Report</h1>
                
                <div class="summary">
                    <h2>Summary Statistics</h2>
                    <p><strong>Total Expectations:</strong> {total_count}</p>
                    <p><strong>Successful:</strong> <span class="success">{success_count}</span></p>
                    <p><strong>Failed:</strong> <span class="failed">{total_count - success_count}</span></p>
                    <p><strong>Success Rate:</strong> <span class="success">{success_pct:.1f}%</span></p>
                    
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: {success_pct}%;">{success_pct:.1f}%</div>
                    </div>
                </div>
                
                <h2>Validation Results Detail</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Expectation Type</th>
                            <th>Column</th>
                            <th>Status</th>
                            <th>Details</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        
        # Add results rows
        for result in results[:100]:  # Limit to first 100 for readability
            expectation_type = result.get("expectation_config", {}).get("expectation_type", "N/A")
            column = result.get("expectation_config", {}).get("kwargs", {}).get("column", "Table-level")
            status = "PASS" if result.get("success") else "FAIL"
            status_class = "status-pass" if result.get("success") else "status-fail"
            
            # Get relevant details
            details = ""
            if not result.get("success"):
                result_dict = result.get("result", {})
                details = f"Unexpected: {result_dict.get('unexpected_count', 'N/A')}"
            
            html_content += f"""
                        <tr>
                            <td>{expectation_type}</td>
                            <td>{column}</td>
                            <td class="{status_class}">{status}</td>
                            <td>{details}</td>
                        </tr>
            """
        
        html_content += """
                    </tbody>
                </table>
            </div>
        </body>
        </html>
        """
        
        # Write to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML report generated: {output_file}")
        return str(output_file.absolute())
        
    except Exception as e:
        logger.error(f"Failed to generate HTML report: {e}")
        logger.debug(f"Error type: {type(e).__name__}")
        raise