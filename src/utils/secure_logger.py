"""
Secure Name Resolver — Enterprise-grade masking utility for data pipeline logs.

Masks sensitive metadata (table names, file paths, column names) in log output
using Table ID alias mapping and column name hashing.

Usage:
    resolver = SecureNameResolver("configs/table_mapping.yaml")
    dataset_id = resolver.get_dataset_id("data/raw/global_health_data.csv")  # → "DS_001"
    masked_col = resolver.mask_column("patient_age")  # → "COL_a1b2c3"

The mapping between masked names and real names is stored in:
    - configs/table_mapping.yaml  (dataset ID ↔ file path)
    - configs/column_mapping.json (COL_hash ↔ real column name) — auto-generated
"""

import hashlib
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

import yaml

logger = logging.getLogger("DQPipeline.secure_logger")


class SecureNameResolver:
    """
    Resolves real table/column names to masked identifiers for secure logging.
    
    Supports:
        - Dataset ID mapping (DS_001 → real file path)
        - Column name hashing (COL_a1b2c3)
        - Config masking (strips sensitive fields before logging)
        - Auto-generates column_mapping.json for internal lookup
    """

    def __init__(self, mapping_path: str = "configs/table_mapping.yaml"):
        self._datasets: Dict[str, Dict] = {}
        self._file_to_id: Dict[str, str] = {}
        self._column_map: Dict[str, str] = {}  # hash → real name
        self._column_reverse: Dict[str, str] = {}  # real name → hash
        self._allow_debug_real_names: bool = False
        self._column_mask_mode: str = "hash"

        self._load_mapping(mapping_path)

    # ─── Loading ──────────────────────────────────────────────────────

    def _load_mapping(self, mapping_path: str) -> None:
        """Load dataset mapping from YAML config."""
        try:
            path = Path(mapping_path)
            if not path.exists():
                logger.warning(
                    "Table mapping config not found. "
                    "All names will use fallback masking."
                )
                return

            with open(path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            self._allow_debug_real_names = config.get(
                "allow_debug_real_names", False
            )
            self._column_mask_mode = config.get("column_mask_mode", "hash")

            for dataset_id, info in config.get("datasets", {}).items():
                self._datasets[dataset_id] = info
                file_path = info.get("file_path", "")
                # Normalize path separators for matching
                normalized = file_path.replace("\\", "/")
                self._file_to_id[normalized] = dataset_id
                self._file_to_id[file_path] = dataset_id

            logger.info(
                f"Secure name resolver initialized with "
                f"{len(self._datasets)} dataset(s)"
            )

        except Exception as e:
            logger.error(f"Failed to load table mapping: {e}")

    # ─── Dataset ID Resolution ────────────────────────────────────────

    def get_dataset_id(self, file_path: str) -> str:
        """
        Get the masked dataset ID for a given file path.

        Args:
            file_path: Real file path (e.g. "data/raw/global_health_data.csv")

        Returns:
            Dataset ID alias (e.g. "DS_001") or a hash-based fallback
        """
        normalized = file_path.replace("\\", "/")

        # Try exact match
        if normalized in self._file_to_id:
            return self._file_to_id[normalized]

        # Try matching just the filename
        for registered_path, dataset_id in self._file_to_id.items():
            if Path(registered_path).name == Path(normalized).name:
                return dataset_id

        # Fallback: hash-based ID
        short_hash = hashlib.sha256(
            normalized.encode()
        ).hexdigest()[:8]
        return f"DS_UNKNOWN_{short_hash}"

    def get_schema_id(self, file_path: str) -> str:
        """Get masked schema reference for a dataset file path."""
        dataset_id = self.get_dataset_id(file_path)
        return f"{dataset_id}_SCHEMA"

    # ─── Column Name Masking ──────────────────────────────────────────

    def mask_column(self, column_name: str) -> str:
        """
        Mask a column name using hash or index mode.

        Args:
            column_name: Real column name

        Returns:
            Masked column name (e.g. "COL_a1b2c3")
        """
        if column_name in self._column_reverse:
            return self._column_reverse[column_name]

        if self._column_mask_mode == "index":
            idx = len(self._column_map) + 1
            masked = f"COL_{idx:03d}"
        else:
            short_hash = hashlib.sha256(
                column_name.encode()
            ).hexdigest()[:6]
            masked = f"COL_{short_hash}"

        self._column_map[masked] = column_name
        self._column_reverse[column_name] = masked
        return masked

    def mask_columns_dict(
        self, schema: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Build masked column mapping for all columns in a schema.
        Also auto-saves the mapping to column_mapping.json.

        Args:
            schema: Schema dictionary { column_name: rules }

        Returns:
            Dict mapping real column name → masked name
        """
        mapping = {}
        for col_name in schema.keys():
            mapping[col_name] = self.mask_column(col_name)

        # Auto-save the reverse lookup for internal debugging
        self._save_column_mapping()

        return mapping

    def _save_column_mapping(self) -> None:
        """Save column mapping to JSON for secure internal lookup."""
        try:
            output_path = Path("configs/column_mapping.json")
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Save as { masked_id: real_name } for easy lookup
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(
                    self._column_map,
                    f,
                    indent=2,
                    ensure_ascii=False,
                )

            logger.info(
                f"Column mapping saved: {len(self._column_map)} columns "
                f"(lookup file: configs/column_mapping.json)"
            )
        except Exception as e:
            logger.warning(f"Failed to save column mapping: {e}")

    # ─── Config Masking ───────────────────────────────────────────────

    def mask_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Return a copy of the config dict with sensitive fields masked.

        Replaces file_path and schema_path with dataset ID aliases.
        Safe to log the result.
        """
        masked = dict(config)

        if "file_path" in masked:
            dataset_id = self.get_dataset_id(masked["file_path"])
            masked["file_path"] = f"[{dataset_id}]"

        if "schema_path" in masked:
            # Use the dataset_id derived from file_path
            file_path = config.get("file_path", "")
            dataset_id = self.get_dataset_id(file_path)
            masked["schema_path"] = f"[{dataset_id}_SCHEMA]"

        return masked

    # ─── Path Masking ─────────────────────────────────────────────────

    def mask_path(self, file_path: str) -> str:
        """
        Mask a file path for logging.

        Args:
            file_path: Real file path

        Returns:
            Masked path string (e.g. "[DS_001]")
        """
        dataset_id = self.get_dataset_id(file_path)
        return f"[{dataset_id}]"

    # ─── Debug Helpers ────────────────────────────────────────────────

    @property
    def allow_debug_real_names(self) -> bool:
        """Whether DEBUG-level logs may show real names."""
        return self._allow_debug_real_names

    def get_column_mapping(self) -> Dict[str, str]:
        """Return the current column mapping (masked → real)."""
        return dict(self._column_map)

    def mask_report_data(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mask column names in a chunk report dict for safe storage.

        Handles:
            - column_profile keys (column names → masked)
            - failed_expectations → expectation_config → kwargs → column
            - Any nested 'column' field in validation results

        Args:
            report: Chunk report dictionary

        Returns:
            New dict with column names masked
        """
        import copy
        masked_report = copy.deepcopy(report)

        # 1. Mask column_profile keys
        if "column_profile" in masked_report:
            original_profile = masked_report["column_profile"]
            masked_profile = {}
            for col_name, col_data in original_profile.items():
                masked_name = self.mask_column(col_name)
                masked_profile[masked_name] = col_data
            masked_report["column_profile"] = masked_profile

        # 2. Mask column names in validation results
        if "failed_expectations" in masked_report:
            for expectation in masked_report["failed_expectations"]:
                if isinstance(expectation, dict):
                    config = expectation.get("expectation_config", {})
                    kwargs = config.get("kwargs", {})
                    if "column" in kwargs:
                        kwargs["column"] = self.mask_column(kwargs["column"])

        # 3. Mask column names in full validation results
        if "validation_results" in masked_report:
            val_results = masked_report["validation_results"]
            if isinstance(val_results, dict) and "results" in val_results:
                for result in val_results["results"]:
                    if isinstance(result, dict):
                        config = result.get("expectation_config", {})
                        kwargs = config.get("kwargs", {})
                        if "column" in kwargs:
                            kwargs["column"] = self.mask_column(
                                kwargs["column"]
                            )

        return masked_report
