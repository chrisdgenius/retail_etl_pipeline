# src/data_quality/quality_checker.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.logger import ETLLogger
from src.config.config import DataQualityConfig
from typing import Dict, List, Tuple
import json

class DataQualityChecker:
    """Comprehensive data quality validation 🎯"""

    def __init__(self, config: DataQualityConfig):
        self.config = config
        self.logger = ETLLogger(__name__)
        self.quality_report = {}

    def run_quality_checks(self, df: DataFrame, dataset_name: str) -> Tuple[bool, Dict]:
        """Run comprehensive data quality checks"""
        self.logger.info(f"🔍 Starting data quality checks for {dataset_name}")

        checks = [
            self._check_null_values,
            self._check_duplicates,
            self._check_data_types,
            self._check_outliers,
            self._check_referential_integrity
        ]

        all_passed = True
        report = {"dataset": dataset_name, "checks": {}}

        for check in checks:
            try:
                check_name = check.__name__.replace("_check_", "")
                passed, details = check(df)

                report["checks"][check_name] = {
                    "passed": passed,
                    "details": details
                }

                if not passed:
                    all_passed = False
                    self.logger.warning(f"⚠️ Quality check failed: {check_name}")
                else:
                    self.logger.info(f"✅ Quality check passed: {check_name}")

            except Exception as e:
                self.logger.error(f"❌ Quality check error in {check.__name__}: {str(e)}")
                all_passed = False

        self.quality_report[dataset_name] = report
        return all_passed, report

    def _check_null_values(self, df: DataFrame) -> Tuple[bool, Dict]:
        """Check for excessive null values"""
        total_rows = df.count()
        null_counts = {}

        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            null_percentage = (null_count / total_rows) * 100
            null_counts[column] = {
                "null_count": null_count,
                "null_percentage": round(null_percentage, 2)
            }

        # Check if any column exceeds threshold
        failed_columns = [
            col_name for col_name, stats in null_counts.items()
            if stats["null_percentage"] > (self.config.null_threshold * 100)
        ]

        return len(failed_columns) == 0, {
            "null_counts": null_counts,
            "failed_columns": failed_columns,
            "threshold_percentage": self.config.null_threshold * 100
        }

    def _check_duplicates(self, df: DataFrame) -> Tuple[bool, Dict]:
        """Check for duplicate records"""
        total_rows = df.count()
        unique_rows = df.distinct().count()
        duplicate_count = total_rows - unique_rows
        duplicate_percentage = (duplicate_count / total_rows) * 100

        passed = duplicate_percentage <= (self.config.duplicate_threshold * 100)

        return passed, {
            "total_rows": total_rows,
            "unique_rows": unique_rows,
            "duplicate_count": duplicate_count,
            "duplicate_percentage": round(duplicate_percentage, 2),
            "threshold_percentage": self.config.duplicate_threshold * 100
        }

    def _check_data_types(self, df: DataFrame) -> Tuple[bool, Dict]:
        """Validate data types and formats"""
        type_issues = []

        for column, data_type in df.dtypes:
            if data_type in ["string", "varchar"]:
                # Check for potential numeric columns stored as strings
                numeric_pattern = r'^\d+\.?\d*$'
                non_numeric_count = df.filter(
                    col(column).isNotNull() &
                    ~col(column).rlike(numeric_pattern)
                ).count()

                if non_numeric_count == 0 and df.filter(col(column).isNotNull()).count() > 0:
                    type_issues.append(f"{column} might be numeric but stored as string")

        return len(type_issues) == 0, {"issues": type_issues}

    def _check_outliers(self, df: DataFrame) -> Tuple[bool, Dict]:
        """Detect outliers in numeric columns"""
        numeric_columns = [col_name for col_name, dtype in df.dtypes if dtype in ["int", "bigint", "float", "double"]]
        outlier_report = {}

        for column in numeric_columns:
            stats = df.select(
                mean(col(column)).alias("mean"),
                stddev(col(column)).alias("stddev")
            ).collect()[0]

            if stats["stddev"] and stats["stddev"] > 0:
                lower_bound = stats["mean"] - (self.config.outlier_threshold * stats["stddev"])
                upper_bound = stats["mean"] + (self.config.outlier_threshold * stats["stddev"])

                outlier_count = df.filter(
                    (col(column) < lower_bound) | (col(column) > upper_bound)
                ).count()

                total_count = df.filter(col(column).isNotNull()).count()
                outlier_percentage = (outlier_count / total_count) * 100 if total_count > 0 else 0

                outlier_report[column] = {
                    "outlier_count": outlier_count,
                    "outlier_percentage": round(outlier_percentage, 2),
                    "lower_bound": round(lower_bound, 2),
                    "upper_bound": round(upper_bound, 2)
                }

        return True, outlier_report  # Outliers don't fail the check, just report them

    def _check_referential_integrity(self, df: DataFrame) -> Tuple[bool, Dict]:
        """Basic referential integrity checks"""
        # This would be expanded based on specific business rules
        integrity_issues = []

        # Example: Check if customer_id exists in transactions
        if "customer_id" in df.columns and "transaction_id" in df.columns:
            null_customer_ids = df.filter(col("customer_id").isNull()).count()
            if null_customer_ids > 0:
                integrity_issues.append(f"Found {null_customer_ids} transactions with null customer_id")

        return len(integrity_issues) == 0, {"issues": integrity_issues}

    def save_quality_report(self, output_path: str):
        """Save quality report to file"""
        try:
            with open(output_path, 'w') as f:
                json.dump(self.quality_report, f, indent=2, default=str)
            self.logger.info(f"📊 Quality report saved to {output_path}")
        except Exception as e:
            self.logger.error(f"❌ Failed to save quality report: {str(e)}")
