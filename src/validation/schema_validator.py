"""
Enhanced Schema Validator with comprehensive error handling and reporting
"""
import great_expectations as ge
import logging
from typing import Dict, Any, Optional, Callable
import traceback

logger = logging.getLogger("DQPipeline.schema_validator")


def validate_schema(
    df, 
    schema: Dict[str, Any],
    result_format: str = "COMPLETE",
    column_masker: Optional[Callable[[str], str]] = None
) -> Dict[str, Any]:
    """
    Validate DataFrame against schema using Great Expectations
    
    Args:
        df: DataFrame to validate
        schema: Schema dictionary with validation rules
        result_format: Great Expectations result format ("COMPLETE", "SUMMARY", "BASIC")
        
    Returns:
        Dictionary with validation results
    """
    try:
        logger.debug(f"Validating schema for DataFrame with {len(df)} rows, {len(df.columns)} columns")
        
        # Convert to Great Expectations DataFrame
        ge_df = ge.from_pandas(df)
        
        # Validate column count
        logger.debug("Validating column count...")
        ge_df.expect_table_column_count_to_equal(
            len(schema),
            result_format=result_format
        )
        
        # Validate each column
        for col_idx, (col, rules) in enumerate(schema.items(), 1):
            try:
                _validate_column(ge_df, col, rules, result_format, column_masker)
                
                if col_idx % 100 == 0:  # Log progress for large schemas
                    logger.debug(f"Validated {col_idx}/{len(schema)} columns")
                    
            except Exception as e:
                masked_col = column_masker(col) if column_masker else col
                logger.error(f"Error validating column '{masked_col}': {e}")
                logger.debug(f"Error type: {type(e).__name__}")
                # Continue with other columns
        
        # Get validation results
        logger.debug("Compiling validation results...")
        validation_results = ge_df.validate()
        
        # ════════════════════════════════════════════════════════════
        # CONVERT GE OBJECTS TO PLAIN DICTIONARIES (JSON serializable)
        # ════════════════════════════════════════════════════════════
        
        # Convert to dictionary if it's a GE object
        if hasattr(validation_results, 'to_json_dict'):
            validation_dict = validation_results.to_json_dict()
        else:
            validation_dict = dict(validation_results)
        
        # Ensure results array contains plain dicts
        if "results" in validation_dict:
            serializable_results = []
            for result in validation_dict["results"]:
                if hasattr(result, 'to_json_dict'):
                    serializable_results.append(result.to_json_dict())
                elif isinstance(result, dict):
                    serializable_results.append(result)
                else:
                    # Convert to dict manually
                    serializable_results.append({
                        "success": getattr(result, "success", False),
                        "expectation_config": getattr(result, "expectation_config", {}),
                        "result": getattr(result, "result", {})
                    })
            validation_dict["results"] = serializable_results
        
        # ════════════════════════════════════════════════════════════
        
        # Log summary
        success_count = sum(1 for r in validation_dict["results"] if r.get("success", False))
        total_count = len(validation_dict["results"])
        
        logger.info(
            f"Validation complete: {success_count}/{total_count} expectations passed "
            f"({(success_count/total_count)*100:.1f}%)"
        )
        
        return validation_dict
        
    except Exception as e:
        logger.error(f"Critical error during schema validation: {e}")
        logger.debug(f"Error type: {type(e).__name__}")
        
        # Return error result
        return {
            "success": False,
            "results": [{
                "success": False,
                "exception_info": {
                    "raised_exception": True,
                    "exception_message": str(e),
                    "exception_traceback": "[REDACTED — see secure debug logs]"
                }
            }],
            "statistics": {},
            "meta": {"error": "Validation failed with exception"}
        }


def _validate_column(
    ge_df,
    col: str,
    rules: Dict[str, Any],
    result_format: str,
    column_masker: Optional[Callable[[str], str]] = None
) -> None:
    """Validate a single column against its schema rules"""
    masked_col = column_masker(col) if column_masker else col
    
    # 1. Column existence
    ge_df.expect_column_to_exist(
        col,
        result_format=result_format
    )
    
    # 2. Data type validation
    if "dtype" in rules:
        try:
            ge_df.expect_column_values_to_be_of_type(
                col,
                rules["dtype"],
                result_format=result_format
            )
        except Exception as e:
            logger.warning(f"Data type validation failed for '{masked_col}': {e}")
    
    # 3. Null value validation
    if "nullable_pct" in rules:
        try:
            mostly_threshold = 1 - rules["nullable_pct"]
            ge_df.expect_column_values_to_not_be_null(
                col,
                mostly=mostly_threshold,
                result_format=result_format
            )
        except Exception as e:
            logger.warning(f"Null validation failed for '{masked_col}': {e}")
    
    # 4. Uniqueness validation
    if rules.get("unique", False):
        try:
            ge_df.expect_column_values_to_be_unique(
                col,
                result_format=result_format
            )
        except Exception as e:
            logger.warning(f"Uniqueness validation failed for '{masked_col}': {e}")
    
    # 5. Range validation (for numeric columns)
    if "min" in rules and "max" in rules:
        try:
            ge_df.expect_column_values_to_be_between(
                col,
                min_value=rules["min"],
                max_value=rules["max"],
                result_format=result_format
            )
        except Exception as e:
            logger.warning(f"Range validation failed for '{masked_col}': {e}")
    
    # 6. String length validation (if specified)
    if "min_length" in rules:
        try:
            ge_df.expect_column_value_lengths_to_be_between(
                col,
                min_value=rules.get("min_length"),
                max_value=rules.get("max_length"),
                result_format=result_format
            )
        except Exception as e:
            logger.warning(f"String length validation failed for '{masked_col}': {e}")
    
    # 7. Set membership validation (if allowed values specified)
    if "allowed_values" in rules:
        try:
            ge_df.expect_column_values_to_be_in_set(
                col,
                value_set=rules["allowed_values"],
                result_format=result_format
            )
        except Exception as e:
            logger.warning(f"Set membership validation failed for '{masked_col}': {e}")
    
    # 8. Pattern matching (if regex pattern specified)
    if "regex_pattern" in rules:
        try:
            ge_df.expect_column_values_to_match_regex(
                col,
                regex=rules["regex_pattern"],
                result_format=result_format
            )
        except Exception as e:
            logger.warning(f"Regex pattern validation failed for '{masked_col}': {e}")


def create_expectation_suite(
    schema: Dict[str, Any],
    suite_name: str = "health_data_suite"
) -> Any:
    """
    Create a Great Expectations expectation suite from schema
    
    Args:
        schema: Schema dictionary
        suite_name: Name for the expectation suite
        
    Returns:
        Expectation suite object
    """
    try:
        import great_expectations as ge
        from great_expectations.core import ExpectationConfiguration
        from great_expectations.core.expectation_suite import ExpectationSuite
        
        suite = ExpectationSuite(expectation_suite_name=suite_name)
        
        # Add table-level expectations
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_table_column_count_to_equal",
                kwargs={"value": len(schema)}
            )
        )
        
        # Add column-level expectations
        for col, rules in schema.items():
            # Column exists
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_to_exist",
                    kwargs={"column": col}
                )
            )
            
            # Data type
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
            
            # Not null
            if "nullable_pct" in rules:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_not_be_null",
                        kwargs={
                            "column": col,
                            "mostly": 1 - rules["nullable_pct"]
                        }
                    )
                )
            
            # Uniqueness
            if rules.get("unique", False):
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_unique",
                        kwargs={"column": col}
                    )
                )
            
            # Range
            if "min" in rules and "max" in rules:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_between",
                        kwargs={
                            "column": col,
                            "min_value": rules["min"],
                            "max_value": rules["max"]
                        }
                    )
                )
        
        logger.info(f"Created expectation suite '{suite_name}' with {len(suite.expectations)} expectations")
        return suite
        
    except Exception as e:
        logger.error(f"Failed to create expectation suite: {e}")
        raise