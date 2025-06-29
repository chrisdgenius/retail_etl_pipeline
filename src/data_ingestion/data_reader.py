# src/data_ingestion/data_reader.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from src.utils.spark_utils import SparkSessionManager, get_retail_schema
from src.utils.logger import ETLLogger
from typing import Dict, Optional
import boto3
from botocore.exceptions import ClientError

class DataReader:
    """Production-grade data ingestion with error handling 📥"""

    def __init__(self, spark_manager: SparkSessionManager):
        self.spark = spark_manager.get_session()
        self.logger = ETLLogger(__name__)
        self.schemas = get_retail_schema()

    def read_csv_with_schema(self, file_path: str, schema_name: str) -> Optional[DataFrame]:
        """Read CSV with predefined schema and error handling"""
        try:
            self.logger.info(f"📖 Reading CSV file: {file_path}")

            if schema_name not in self.schemas:
                raise ValueError(f"Schema '{schema_name}' not found")

            df = self.spark.read \
                .option("header", "true") \
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
                .option("dateFormat", "yyyy-MM-dd") \
                .schema(self.schemas[schema_name]) \
                .csv(file_path)

            # Add ingestion metadata
            df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("source_file", lit(file_path))

            record_count = df.count()
            self.logger.info(f"✅ Successfully read {record_count} records from {file_path}")

            return df
        except Exception as e:
            self.logger.error(f"❌ Failed to read {file_path}: {str(e)}")
            return None

    def read_from_s3(self, s3_path: str, file_format: str = "parquet") -> Optional[DataFrame]:
        """Read data from S3 with retry logic"""
        try:
            self.logger.info(f"☁️ Reading from S3: {s3_path}")

            if file_format.lower() == "parquet":
                df = self.spark.read.parquet(s3_path)
            elif file_format.lower() == "csv":
                df = self.spark.read.option("header", "true").csv(s3_path)
            elif file_format.lower() == "json":
                df = self.spark.read.json(s3_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")

            self.logger.info(f"✅ Successfully read data from S3")
            return df
        except Exception as e:
            self.logger.error(f"❌ Failed to read from S3: {str(e)}")
            return None

    def validate_data_freshness(self, df: DataFrame, date_column: str, max_age_hours: int = 24) -> bool:
        """Check if data is fresh enough for processing"""
        try:
            latest_date = df.agg(max(col(date_column)).alias("latest_date")).collect()[0]["latest_date"]

            if latest_date:
                hours_old = (datetime.now() - latest_date).total_seconds() / 3600
                is_fresh = hours_old <= max_age_hours

                self.logger.info(f"📅 Data freshness check: {hours_old:.1f} hours old (threshold: {max_age_hours}h)")
                return is_fresh
            return False
        except Exception as e:
            self.logger.error(f"❌ Data freshness validation failed: {str(e)}")
            return False
