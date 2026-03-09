"""
Enhanced reporting modules with error handling and logging
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any
from src.utils.json_utils import make_json_serializable
import traceback

logger = logging.getLogger("DQPipeline.reporting")


def save_chunk_report(chunk_id: int, report: Dict[str, Any]) -> bool:
    """
    Save chunk validation report to JSON file with error handling
    
    Args:
        chunk_id: Chunk identifier
        report: Report dictionary
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create directory if it doesn't exist
        output_dir = Path("outputs/chunk_reports")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Define output path
        path = output_dir / f"chunk_{chunk_id}.json"
        
        # ════════════════════════════════════════════════════════════
        # TRUNCATE LARGE ARRAYS TO PREVENT FILE CORRUPTION
        # ════════════════════════════════════════════════════════════
        
        # Limit failed_expectations to first 100 to prevent huge files
        if "failed_expectations" in report and isinstance(report["failed_expectations"], list):
            if len(report["failed_expectations"]) > 100:
                logger.warning(
                    f"Chunk {chunk_id} has {len(report['failed_expectations'])} failed expectations. "
                    f"Truncating to first 100 for report."
                )
                report["failed_expectations"] = report["failed_expectations"][:100]
                report["failed_expectations_truncated"] = True
        
        # Limit unexpected_list in each failed expectation to 20 items
        if "failed_expectations" in report:
            for expectation in report.get("failed_expectations", []):
                if isinstance(expectation, dict):
                    result = expectation.get("result", {})
                    if isinstance(result, dict):
                        # Truncate unexpected_list
                        if "unexpected_list" in result and isinstance(result["unexpected_list"], list):
                            if len(result["unexpected_list"]) > 20:
                                result["unexpected_list"] = result["unexpected_list"][:20]
                                result["unexpected_list_truncated"] = True
                        
                        # Truncate partial_unexpected_list
                        if "partial_unexpected_list" in result and isinstance(result["partial_unexpected_list"], list):
                            if len(result["partial_unexpected_list"]) > 20:
                                result["partial_unexpected_list"] = result["partial_unexpected_list"][:20]
        
        # ════════════════════════════════════════════════════════════
        
        # Make report JSON serializable
        report = make_json_serializable(report)
        
        # Write to file with explicit flush
        with open(path, "w", encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
            f.flush()  # Force write to disk
        
        logger.debug(f"Chunk report saved: {path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to save chunk report for chunk {chunk_id}: {e}")
        logger.debug(f"Error type: {type(e).__name__}")
        
        # Try to save minimal report as fallback
        try:
            minimal_report = {
                "chunk_id": chunk_id,
                "row_count": report.get("row_count", 0),
                "validation_success": report.get("validation_success", False),
                "failed_expectations_count": report.get("failed_expectations_count", 0),
                "error": f"Failed to save full report: {str(e)}",
                "note": "Check logs for details"
            }
            
            with open(path, "w", encoding='utf-8') as f:
                json.dump(minimal_report, f, indent=2)
                f.flush()
            
            logger.warning(f"Saved minimal report for chunk {chunk_id}")
            
        except Exception as fallback_error:
            logger.error(f"Even fallback save failed: {fallback_error}")
        
        return False


def save_overall_report(report: Dict[str, Any]) -> bool:
    """
    Save overall summary report to JSON file with error handling
    
    Args:
        report: Overall report dictionary
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create directory if it doesn't exist
        output_dir = Path("outputs/summaries")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Make report JSON serializable
        report = make_json_serializable(report)
        
        # Write to file
        output_path = output_dir / "overall_summary.json"
        with open(output_path, "w", encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Overall report saved: {output_path}")
        
        # Also save a human-readable text version
        _save_text_summary(report, output_dir / "overall_summary.txt")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to save overall report: {e}")
        logger.debug(f"Error type: {type(e).__name__}")
        return False


def _save_text_summary(report: Dict[str, Any], output_path: Path) -> None:
    """Save a human-readable text summary"""
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("DATA QUALITY PIPELINE - OVERALL SUMMARY\n")
            f.write("=" * 80 + "\n\n")
            
            if "pipeline_metadata" in report:
                f.write("PIPELINE METADATA\n")
                f.write("-" * 80 + "\n")
                for key, value in report["pipeline_metadata"].items():
                    f.write(f"{key.replace('_', ' ').title()}: {value}\n")
                f.write("\n")
            
            if "summary_statistics" in report:
                f.write("SUMMARY STATISTICS\n")
                f.write("-" * 80 + "\n")
                for key, value in report["summary_statistics"].items():
                    f.write(f"{key.replace('_', ' ').title()}: {value:,}\n")
                f.write("\n")
            
            if "performance_metrics" in report:
                f.write("PERFORMANCE METRICS\n")
                f.write("-" * 80 + "\n")
                for key, value in report["performance_metrics"].items():
                    f.write(f"{key.replace('_', ' ').title()}: {value:,}\n")
                f.write("\n")
            
            if "failed_chunk_ids" in report and report["failed_chunk_ids"]:
                f.write("FAILED CHUNKS\n")
                f.write("-" * 80 + "\n")
                f.write(f"Failed chunk IDs: {', '.join(map(str, report['failed_chunk_ids']))}\n")
            
            f.write("=" * 80 + "\n")
        
        logger.debug(f"Text summary saved: {output_path}")
        
    except Exception as e:
        logger.warning(f"Failed to save text summary: {e}")


def load_chunk_report(chunk_id: int) -> Dict[str, Any]:
    """
    Load a chunk report from file
    
    Args:
        chunk_id: Chunk identifier
        
    Returns:
        Report dictionary or None if not found
    """
    try:
        path = Path("outputs/chunk_reports") / f"chunk_{chunk_id}.json"
        
        if not path.exists():
            logger.warning(f"Chunk report not found: {path}")
            return None
        
        with open(path, 'r', encoding='utf-8') as f:
            report = json.load(f)
        
        logger.debug(f"Loaded chunk report: {path}")
        return report
        
    except Exception as e:
        logger.error(f"Failed to load chunk report for chunk {chunk_id}: {e}")
        return None


def aggregate_chunk_reports(num_chunks: int) -> Dict[str, Any]:
    """
    Aggregate statistics from all chunk reports
    
    Args:
        num_chunks: Number of chunks to aggregate
        
    Returns:
        Aggregated statistics dictionary
    """
    try:
        logger.info(f"Aggregating {num_chunks} chunk reports...")
        
        aggregated = {
            "total_chunks": num_chunks,
            "loaded_chunks": 0,
            "total_rows": 0,
            "successful_chunks": 0,
            "failed_chunks": 0,
            "total_failed_expectations": 0,
            "column_statistics": {}
        }
        
        for chunk_id in range(1, num_chunks + 1):
            report = load_chunk_report(chunk_id)
            
            if report is None:
                continue
            
            aggregated["loaded_chunks"] += 1
            aggregated["total_rows"] += report.get("row_count", 0)
            
            if report.get("validation_success", False):
                aggregated["successful_chunks"] += 1
            else:
                aggregated["failed_chunks"] += 1
            
            aggregated["total_failed_expectations"] += report.get("failed_expectations_count", 0)
        
        logger.info(f"Aggregated {aggregated['loaded_chunks']}/{num_chunks} chunk reports")
        return aggregated
        
    except Exception as e:
        logger.error(f"Failed to aggregate chunk reports: {e}")
        logger.debug(f"Error type: {type(e).__name__}")
        return {}