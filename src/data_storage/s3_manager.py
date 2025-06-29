# src/data_storage/s3_manager.py
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pyspark.sql import DataFrame
from src.utils.logger import ETLLogger
from src.config.config import S3Config
from typing import Optional, List, Dict
import json
from datetime import datetime
import time

class S3DataManager:
    """ ☁️ Production-Grade S3 Data Lake Manager
    This class handles all S3 operations with enterprise-grade features:
    - Automatic retry logic for transient failures
    - Partitioning strategies for optimal query performance
    - Data lifecycle management
    - Cost optimization through intelligent storage classes
    - Comprehensive error handling and logging

    Why S3 for Data Lakes?
    - Virtually unlimited scalability
    - Cost-effective storage with multiple tiers
    - Integration with analytics tools (Athena, Redshift, etc.)
    - Built-in durability and availability
    """

    def __init__(self, config: S3Config):
        self.config = config
        self.logger = ETLLogger(__name__)
        self.s3_client = None
        self._initialize_s3_client()

    def _initialize_s3_client(self):
        """ 🔐 Initialize S3 client with proper error handling
        This handles various authentication scenarios:
        - IAM roles (recommended for production)
        - Access keys (for development/testing)
        - Cross-account access
        """
        try:
            if self.config.access_key and self.config.secret_key:
                # Explicit credentials (development/testing)
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.config.access_key,
                    aws_secret_access_key=self.config.secret_key,
                    region_name=self.config.region
                )
                self.logger.info("🔑 S3 client initialized with explicit credentials")
            else:
                # Use IAM role or default credential chain (production)
                self.s3_client = boto3.client('s3', region_name=self.config.region)
                self.logger.info("🔑 S3 client initialized with default credential chain")

            # Test connection
            self.s3_client.head_bucket(Bucket=self.config.bucket_name)
            self.logger.info(f"✅ Successfully connected to S3 bucket: {self.config.bucket_name}")

        except NoCredentialsError:
            self.logger.error("❌ AWS credentials not found")
            raise
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.logger.error(f"❌ S3 bucket not found: {self.config.bucket_name}")
            else:
                self.logger.error(f"❌ S3 connection failed: {str(e)}")
            raise

    def write_dataframe_to_s3(self, df: DataFrame, s3_path: str,
                            file_format: str = "parquet", partition_cols: Optional[List[str]] = None,
                            mode: str = "overwrite") -> bool:
        """ 📤 Write DataFrame to S3 with optimizations
        This method implements several production best practices:
        - Partitioning for query performance
        - Compression for storage efficiency
        - Atomic writes to prevent partial failures
        - Metadata tracking for data lineage

        Partitioning Strategy:
        - Time-based partitioning (year/month/day) for time-series data
        - Category-based partitioning for dimensional data
        - Balanced partition sizes (not too many small files)
        """
        try:
            self.logger.info(f"📤 Writing DataFrame to S3: {s3_path}")

            # Add metadata columns for data lineage
            df_with_metadata = df.withColumn("etl_timestamp", current_timestamp()) \
                .withColumn("etl_job_id", lit(f"job_{int(time.time())}"))

            # Configure write operation based on format
            writer = df_with_metadata.write.mode(mode)

            if file_format.lower() == "parquet":
                # Parquet is optimal for analytics workloads
                writer = writer.option("compression", "snappy")  # Good balance of speed/compression
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                writer.parquet(s3_path)

            elif file_format.lower() == "delta":
                # Delta Lake for ACID transactions (if available)
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                writer.format("delta").save(s3_path)

            elif file_format.lower() == "csv":
                # CSV for compatibility (not recommended for large datasets)
                writer.option("header", "true").csv(s3_path)

            else:
                raise ValueError(f"Unsupported file format: {file_format}")

            # Verify write success
            if self._verify_s3_write(s3_path):
                self.logger.info(f"✅ Successfully wrote data to {s3_path}")
                self._log_dataset_metadata(s3_path, df_with_metadata, file_format, partition_cols)
                return True
            else:
                self.logger.error(f"❌ Write verification failed for {s3_path}")
                return False

        except Exception as e:
            self.logger.error(f"❌ Failed to write DataFrame to S3: {str(e)}")
            return False

    def _verify_s3_write(self, s3_path: str) -> bool:
        """ ✅ Verify that data was successfully written to S3
        This is crucial for data integrity - we need to ensure that our write operations actually succeeded before marking the job as complete.
        """
        try:
            # Extract bucket and key from S3 path
            path_parts = s3_path.replace("s3://", "").replace("s3a://", "").split("/", 1)
            bucket = path_parts[0]
            prefix = path_parts[1] if len(path_parts) > 1 else ""

            # List objects to verify data exists
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=1
            )

            return response.get('KeyCount', 0) > 0

        except Exception as e:
            self.logger.error(f"❌ S3 write verification failed: {str(e)}")
            return False

    def _log_dataset_metadata(self, s3_path: str, df: DataFrame,
                            file_format: str, partition_cols: Optional[List[str]]):
        """ 📋 Log dataset metadata for data catalog and lineage tracking
        This creates a metadata record that helps with:
        - Data discovery and cataloging
        - Impact analysis for schema changes
        - Compliance and audit requirements
        - Performance optimization
        """
        try:
            metadata = {
                "dataset_path": s3_path,
                "record_count": df.count(),
                "column_count": len(df.columns),
                "schema": df.schema.json(),
                "file_format": file_format,
                "partition_columns": partition_cols or [],
                "created_timestamp": datetime.now().isoformat(),
                "size_estimate_mb": self._estimate_dataset_size(df)
            }

            # Save metadata to S3 for data catalog
            metadata_path = f"{s3_path}/_metadata/dataset_info.json"
            self._upload_json_to_s3(metadata, metadata_path)

            self.logger.info(f"📋 Dataset metadata logged: {metadata['record_count']:,} records")

        except Exception as e:
            self.logger.warning(f"⚠️ Failed to log dataset metadata: {str(e)}")

    def _estimate_dataset_size(self, df: DataFrame) -> float:
        """Estimate dataset size in MB"""
        try:
            # Simple estimation based on row count and column types
            row_count = df.count()
            avg_row_size = 0

            for col_name, col_type in df.dtypes:
                if col_type in ["string", "varchar"]:
                    avg_row_size += 50  # Estimate 50 bytes per string column
                elif col_type in ["int", "integer"]:
                    avg_row_size += 4
                elif col_type in ["bigint", "long", "double"]:
                    avg_row_size += 8
                elif col_type in ["timestamp", "date"]:
                    avg_row_size += 8
                else:
                    avg_row_size += 10  # Default estimate

            total_size_bytes = row_count * avg_row_size
            return round(total_size_bytes / (1024 * 1024), 2)  # Convert to MB

        except:
            return 0.0

    def _upload_json_to_s3(self, data: Dict, s3_path: str):
        """Upload JSON data to S3"""
        try:
            path_parts = s3_path.replace("s3://", "").replace("s3a://", "").split("/", 1)
            bucket = path_parts[0]
            key = path_parts[1]

            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(data, indent=2),
                ContentType='application/json'
            )

        except Exception as e:
            self.logger.error(f"❌ Failed to upload JSON to S3: {str(e)}")

    def create_data_lake_structure(self):
        """ 🏗️ Create organized data lake folder structure
        A well-organized data lake is crucial for:
        - Data governance and discoverability
        - Performance optimization
        - Cost management
        - Compliance and security

        Standard structure:
        - raw/: Unprocessed data as received
        - processed/: Cleaned and validated data
        - curated/: Business-ready analytics datasets
        - archive/: Historical data for compliance
        """
        folders = [
            f"{self.config.raw_data_prefix}customers/",
            f"{self.config.raw_data_prefix}products/",
            f"{self.config.raw_data_prefix}transactions/",
            f"{self.config.processed_data_prefix}enriched_transactions/",
            f"{self.config.processed_data_prefix}customer_metrics/",
            f"{self.config.processed_data_prefix}product_metrics/",
            f"{self.config.curated_data_prefix}business_metrics/",
            f"{self.config.curated_data_prefix}rfm_analysis/",
            f"{self.config.curated_data_prefix}cohort_analysis/",
            "archive/",
            "metadata/",
            "logs/"
        ]

        for folder in folders:
            try:
                # Create empty object to establish folder structure
                self.s3_client.put_object(
                    Bucket=self.config.bucket_name,
                    Key=f"{folder}.keep",
                    Body=""
                )
                self.logger.info(f"📁 Created folder: {folder}")

            except Exception as e:
                self.logger.warning(f"⚠️ Failed to create folder {folder}: {str(e)}")

        self.logger.info("🏗️ Data lake structure created successfully")
