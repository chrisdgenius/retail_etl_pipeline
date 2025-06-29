# src/config/config.py
import os
from dataclasses import dataclass
from typing import Dict, Any
import yaml

@dataclass
class SparkConfig:
    """Spark configuration settings ⚡"""
    app_name: str = "RetailETLPipeline"
    master: str = "local[*]"
    executor_memory: str = "4g"
    driver_memory: str = "2g"
    max_result_size: str = "1g"
    serializer: str = "org.apache.spark.serializer.KryoSerializer"

@dataclass
class S3Config:
    """AWS S3 configuration 🪣"""
    bucket_name: str = "retail-analytics-bucket"
    raw_data_prefix: str = "raw/"
    processed_data_prefix: str = "processed/"
    curated_data_prefix: str = "curated/"
    access_key: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    secret_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    region: str = "us-east-1"

@dataclass
class DataQualityConfig:
    """Data quality thresholds 🎯"""
    null_threshold: float = 0.05  # 5% null values allowed
    duplicate_threshold: float = 0.01  # 1% duplicates allowed
    outlier_threshold: float = 3.0  # 3 standard deviations

class ConfigManager:
    """Central configuration manager 🎛️"""

    def __init__(self, config_path: str = "src/config/pipeline_config.yaml"):
        self.config_path = config_path
        self.spark_config = SparkConfig()
        self.s3_config = S3Config()
        self.data_quality_config = DataQualityConfig()
        self._load_config()

    def _load_config(self):
        """Load configuration from YAML file"""
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r') as file:
                config_data = yaml.safe_load(file)
            self._update_configs(config_data)

    def _update_configs(self, config_data: Dict[str, Any]):
        """Update configuration objects with YAML data"""
        if 'spark' in config_data:
            for key, value in config_data['spark'].items():
                if hasattr(self.spark_config, key):
                    setattr(self.spark_config, key, value)

        if 's3' in config_data:
            for key, value in config_data['s3'].items():
                if hasattr(self.s3_config, key):
                    setattr(self.s3_config, key, value)
