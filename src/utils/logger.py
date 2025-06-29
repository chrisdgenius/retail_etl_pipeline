# src/utils/logger.py
import logging
import os
from datetime import datetime
from typing import Optional

class ETLLogger:
    """Production-grade logging system 📝"""

    def __init__(self, name: str, log_level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))

        if not self.logger.handlers:
            self._setup_handlers()

    def _setup_handlers(self):
        """Setup file and console handlers"""
        # Create logs directory if it doesn't exist
        os.makedirs("logs", exist_ok=True)

        # File handler
        log_filename = f"logs/etl_pipeline_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.DEBUG)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def info(self, message: str, **kwargs):
        self.logger.info(message, extra=kwargs)

    def error(self, message: str, **kwargs):
        self.logger.error(message, extra=kwargs)

    def warning(self, message: str, **kwargs):
        self.logger.warning(message, extra=kwargs)

    def debug(self, message: str, **kwargs):
        self.logger.debug(message, extra=kwargs)

# src/utils/spark_utils.py
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from src.config.config import ConfigManager
from src.utils.logger import ETLLogger

class SparkSessionManager:
    """Spark session management with best practices ⚡"""

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager.spark_config
        self.logger = ETLLogger(__name__)
        self._session = None

    def get_session(self) -> SparkSession:
        """Get or create Spark session"""
        if self._session is None:
            self.logger.info("🚀 Creating new Spark session...")

            self._session = SparkSession.builder \
                .appName(self.config.app_name) \
                .master(self.config.master) \
                .config("spark.executor.memory", self.config.executor_memory) \
                .config("spark.driver.memory", self.config.driver_memory) \
                .config("spark.driver.maxResultSize", self.config.max_result_size) \
                .config("spark.serializer", self.config.serializer) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()

            # Set log level to reduce noise
            self._session.sparkContext.setLogLevel("WARN")
            self.logger.info("✅ Spark session created successfully")

        return self._session

    def stop_session(self):
        """Stop Spark session"""
        if self._session:
            self._session.stop()
            self._session = None
            self.logger.info("🛑 Spark session stopped")

def get_retail_schema():
    """Define schemas for our retail data 📋"""
    customer_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("registration_date", DateType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("age_group", StringType(), True),
        StructField("customer_segment", StringType(), True)
    ])

    product_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("cost", DoubleType(), True),
        StructField("launch_date", DateType(), True)
    ])

    transaction_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("payment_method", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("status", StringType(), True)
    ])

    return {
        "customers": customer_schema,
        "products": product_schema,
        "transactions": transaction_schema
    }
