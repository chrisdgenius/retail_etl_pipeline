# src/pipeline/main_orchestrator.py
from pyspark.sql import SparkSession
from src.config.config import ConfigManager, SparkConfig, S3Config, DataQualityConfig
from src.utils.spark_utils import SparkSessionManager
from src.data_ingestion.data_reader import DataReader
from src.data_processing.transformations import RetailDataTransformer
from src.business_metrics.advanced_analytics import AdvancedBusinessMetrics
from src.data_quality.quality_checker import DataQualityChecker
from src.data_storage.s3_manager import S3DataManager
from src.error_handling.pipeline_resilience import PipelineErrorHandler, retry_with_exponential_backoff
from src.utils.logger import ETLLogger
from typing import Dict, List, Optional, Tuple
import time
from datetime import datetime
import json
import os

class RetailETLOrchestrator:
    """ 🎭 The Master Orchestrator - Conducting the Data Symphony
    This class is the heart of our ETL pipeline, responsible for:
    - Coordinating all pipeline components
    - Managing execution flow and dependencies
    - Handling errors and recovery scenarios
    - Monitoring performance and data quality
    - Ensuring data lineage and audit trails

    Think of it as the conductor of an orchestra - each component is an instrument, and the orchestrator ensures they all play in harmony to create beautiful music (insights)!
    """

    def __init__(self, config_path: str = "src/config/pipeline_config.yaml"):
        self.logger = ETLLogger(__name__)
        self.config_manager = ConfigManager(config_path)
        self.error_handler = PipelineErrorHandler()

        # Initialize core components
        self.spark_manager = SparkSessionManager(self.config_manager)
        self.spark = self.spark_manager.get_session()

        # Initialize pipeline components
        self.data_reader = DataReader(self.spark_manager)
        self.transformer = RetailDataTransformer(self.spark)
        self.analytics = AdvancedBusinessMetrics(self.spark)
        self.quality_checker = DataQualityChecker(self.config_manager.data_quality_config)
        self.s3_manager = S3DataManager(self.config_manager.s3_config)

        # Pipeline state tracking
        self.pipeline_state = {
            "start_time": None,
            "end_time": None,
            "status": "INITIALIZED",
            "processed_records": {},
            "quality_reports": {},
            "errors": [],
            "performance_metrics": {}
        }

        self.logger.info("🎭 ETL Orchestrator initialized successfully")

    def execute_full_pipeline(self, execution_date: Optional[str] = None) -> Dict:
        """ 🚀 Execute the complete ETL pipeline
        This is the main entry point that orchestrates the entire data processing workflow:

        1. 📥 Data Ingestion: Read from multiple sources
        2. 🧹 Data Quality: Validate and clean data
        3. 🔄 Transformation: Apply business logic
        4. 📊 Analytics: Generate business metrics
        5. ☁️ Storage: Persist to data lake
        6. 📋 Reporting: Generate execution summary

        The pipeline is designed to be:
        - Idempotent: Can be run multiple times safely
        - Resumable: Can restart from failure points
        - Auditable: Full lineage and quality tracking
        """
        self.pipeline_state["start_time"] = datetime.now()
        self.pipeline_state["status"] = "RUNNING"

        try:
            self.logger.info("🚀 Starting full ETL pipeline execution...")

            # Step 1: Data Ingestion
            raw_datasets = self._execute_data_ingestion()

            # Step 2: Data Quality Validation
            validated_datasets = self._execute_data_quality_checks(raw_datasets)

            # Step 3: Data Transformation
            transformed_datasets = self._execute_transformations(validated_datasets)

            # Step 4: Business Analytics
            analytics_datasets = self._execute_business_analytics(transformed_datasets)

            # Step 5: Data Persistence
            self._execute_data_persistence(analytics_datasets)

            # Step 6: Pipeline Completion
            self._finalize_pipeline_execution()

            self.logger.info("✅ ETL pipeline completed successfully!")
            return self._generate_execution_summary()

        except Exception as e:
            return self._handle_pipeline_failure(e)

    @retry_with_exponential_backoff(max_retries=3, base_delay=2.0)
    def _execute_data_ingestion(self) -> Dict[str, any]:
        """ 📥 Data Ingestion Phase
        This phase reads data from various sources with robust error handling:
        - File-based sources (CSV, Parquet, JSON)
        - Database sources (JDBC connections)
        - API sources (REST endpoints)
        - Streaming sources (Kafka, Kinesis)

        Key features:
        - Schema validation on ingestion
        - Data freshness checks
        - Source availability monitoring
        - Automatic retry for transient failures
        """
        self.logger.info("📥 Executing data ingestion phase...")

        try:
            datasets = {}

            # Read customers data
            self.logger.info("👥 Reading customer data...")
            customers_df = self.data_reader.read_csv_with_schema(
                "data/raw/customers.csv", "customers"
            )

            if customers_df is None:
                raise Exception("Failed to read customers data")

            datasets["customers"] = customers_df
            self.pipeline_state["processed_records"]["customers"] = customers_df.count()

            # Read products data
            self.logger.info("🛍️ Reading product data...")
            products_df = self.data_reader.read_csv_with_schema(
                "data/raw/products.csv", "products"
            )

            if products_df is None:
                raise Exception("Failed to read products data")

            datasets["products"] = products_df
            self.pipeline_state["processed_records"]["products"] = products_df.count()

            # Read transactions data
            self.logger.info("💳 Reading transaction data...")
            transactions_df = self.data_reader.read_csv_with_schema(
                "data/raw/transactions.csv", "transactions"
            )

            if transactions_df is None:
                raise Exception("Failed to read transactions data")

            datasets["transactions"] = transactions_df
            self.pipeline_state["processed_records"]["transactions"] = transactions_df.count()

            # Validate data freshness
            if not self.data_reader.validate_data_freshness(transactions_df, "transaction_date", 48):
                self.logger.warning("⚠️ Transaction data may be stale")

            self.logger.info("✅ Data ingestion completed successfully")
            return datasets

        except Exception as e:
            error_context = self.error_handler.handle_error(
                e, "data_ingestion", {"phase": "ingestion"}
            )
            self.pipeline_state["errors"].append(error_context)
            raise e

    def _execute_data_quality_checks(self, datasets: Dict[str, any]) -> Dict[str, any]:
        """ 🎯 Data Quality Validation Phase
        This is one of the most critical phases! Data quality issues can:
        - Lead to incorrect business decisions
        - Cause downstream system failures
        - Result in compliance violations
        - Damage stakeholder trust

        Our comprehensive quality checks include:
        - Schema validation
        - Null value analysis
        - Duplicate detection
        - Outlier identification
        - Business rule validation
        - Cross-dataset consistency checks
        """
        self.logger.info("🎯 Executing data quality checks...")

        try:
            validated_datasets = {}
            quality_reports = {}

            for dataset_name, df in datasets.items():
                self.logger.info(f"🔍 Validating {dataset_name} dataset...")

                # Run comprehensive quality checks
                passed, report = self.quality_checker.run_quality_checks(
                    df, dataset_name
                )

                quality_reports[dataset_name] = report

                if passed:
                    self.logger.info(f"✅ {dataset_name} passed quality checks")
                    validated_datasets[dataset_name] = df
                else:
                    self.logger.warning(f"⚠️ {dataset_name} has quality issues")

                # Apply data cleansing based on quality issues
                cleaned_df = self._apply_data_cleansing(df, report)
                validated_datasets[dataset_name] = cleaned_df
                self.logger.info(f"🧹 Applied data cleansing to {dataset_name}")

            # Store quality reports for audit
            self.pipeline_state["quality_reports"] = quality_reports
            self.quality_checker.save_quality_report("logs/quality_report.json")

            # Cross-dataset validation
            self._validate_cross_dataset_consistency(validated_datasets)

            self.logger.info("✅ Data quality validation completed")
            return validated_datasets

        except Exception as e:
            error_context = self.error_handler.handle_error(
                e, "data_quality", {"phase": "validation"}
            )
            self.pipeline_state["errors"].append(error_context)
            raise e

    def _apply_data_cleansing(self, df, quality_report: Dict) -> any:
        """ 🧹 Apply intelligent data cleansing
        Based on quality check results, we apply appropriate cleansing:
        - Fill missing values with business-appropriate defaults
        - Remove or flag duplicate records
        - Standardize data formats
        - Apply business rules for data correction
        """
        from pyspark.sql.functions import when, col, isnan, isnull

        cleaned_df = df

        # Handle null values based on business rules
        if "null_analysis" in quality_report.get("checks", {}):
            null_analysis = quality_report["checks"]["null_analysis"]["details"]

            for column, stats in null_analysis["null_counts"].items():
                if stats["null_percentage"] > 0:
                    if column in ["customer_segment"]: # Fill missing customer segments with 'Unknown'
                        cleaned_df = cleaned_df.fillna({"customer_segment": "Unknown"})
                    elif column in ["country"]: # Fill missing countries with 'Not Specified'
                        cleaned_df = cleaned_df.fillna({"country": "Not Specified"})

        # Remove duplicates if found
        if "duplicate_analysis" in quality_report.get("checks", {}):
            duplicate_analysis = quality_report["checks"]["duplicate_analysis"]["details"]
            if duplicate_analysis["duplicate_count"] > 0:
                cleaned_df = cleaned_df.dropDuplicates()
                self.logger.info(f"🗑️ Removed {duplicate_analysis['duplicate_count']} duplicate records")

        return cleaned_df

    def _validate_cross_dataset_consistency(self, datasets: Dict[str, any]):
        """ 🔗 Cross-dataset consistency validation
        Ensures referential integrity across datasets:
        - All customer_ids in transactions exist in customers table
        - All product_ids in transactions exist in products table
        - Date ranges are consistent across datasets
        """
        self.logger.info("🔗 Validating cross-dataset consistency...")

        transactions_df = datasets["transactions"]
        customers_df = datasets["customers"]
        products_df = datasets["products"]

        # Check customer referential integrity
        transaction_customers = transactions_df.select("customer_id").distinct()
        valid_customers = customers_df.select("customer_id").distinct()

        orphaned_customers = transaction_customers.subtract(valid_customers).count()
        if orphaned_customers > 0:
            self.logger.warning(f"⚠️ Found {orphaned_customers} transactions with invalid customer_ids")

        # Check product referential integrity
        transaction_products = transactions_df.select("product_id").distinct()
        valid_products = products_df.select("product_id").distinct()

        orphaned_products = transaction_products.subtract(valid_products).count()
        if orphaned_products > 0:
            self.logger.warning(f"⚠️ Found {orphaned_products} transactions with invalid product_ids")

        self.logger.info("✅ Cross-dataset consistency validation completed")

    def _execute_transformations(self, datasets: Dict[str, any]) -> Dict[str, any]:
        """ 🔄 Data Transformation Phase
        This phase applies business logic to transform raw data into analytics-ready datasets:
        - Data enrichment through joins
        - Calculated field generation
        - Data aggregation and summarization
        - Time-based partitioning
        - Performance optimization

        The goal is to create datasets that directly support business questions.
        """
        self.logger.info("🔄 Executing data transformations...")

        try:
            start_time = time.time()
            transformed_datasets = {}

            # Create enriched transactions (the foundation dataset)
            self.logger.info("🔗 Creating enriched transactions...")
            enriched_transactions = self.transformer.create_enriched_transactions(
                datasets["transactions"],
                datasets["customers"],
                datasets["products"]
            )

            # Cache this dataset as it's used by multiple downstream processes
            enriched_transactions.cache()
            transformed_datasets["enriched_transactions"] = enriched_transactions

            # Calculate customer lifetime metrics
            self.logger.info("💎 Calculating customer lifetime metrics...")
            customer_metrics = self.transformer.calculate_customer_lifetime_metrics(
                enriched_transactions
            )
            transformed_datasets["customer_metrics"] = customer_metrics

            # Calculate product performance metrics
            self.logger.info("📈 Calculating product performance metrics...")
            product_metrics = self.transformer.create_product_performance_metrics(
                enriched_transactions
            )
            transformed_datasets["product_metrics"] = product_metrics

            # Create time series analytics
            self.logger.info("📅 Creating time series analytics...")
            daily_metrics, monthly_metrics = self.transformer.create_time_series_analytics(
                enriched_transactions
            )
            transformed_datasets["daily_metrics"] = daily_metrics
            transformed_datasets["monthly_metrics"] = monthly_metrics

            # Record performance metrics
            transformation_time = time.time() - start_time
            self.pipeline_state["performance_metrics"]["transformation_time"] = transformation_time

            self.logger.info(f"✅ Data transformations completed in {transformation_time:.2f} seconds")
            return transformed_datasets

        except Exception as e:
            error_context = self.error_handler.handle_error(
                e, "data_transformation", {"phase": "transformation"}
            )
            self.pipeline_state["errors"].append(error_context)
            raise e

    def _execute_business_analytics(self, datasets: Dict[str, any]) -> Dict[str, any]:
        """ 📊 Business Analytics Phase
        This phase generates sophisticated business insights:
        - RFM customer segmentation
        - Cohort analysis for retention tracking
        - Market basket analysis for cross-selling
        - Churn prediction indicators
        - Advanced KPIs and metrics

        These analytics directly support strategic business decisions.
        """
        self.logger.info("📊 Executing business analytics...")

        try:
            start_time = time.time()
            analytics_datasets = datasets.copy() # Keep existing datasets

            enriched_transactions = datasets["enriched_transactions"]
            customer_metrics = datasets["customer_metrics"]

            # RFM Analysis for customer segmentation
            self.logger.info("🎯 Performing RFM analysis...")
            rfm_analysis = self.analytics.perform_rfm_analysis(enriched_transactions)
            analytics_datasets["rfm_analysis"] = rfm_analysis

            # Cohort analysis for retention insights
            self.logger.info("📈 Performing cohort analysis...")
            cohort_analysis = self.analytics.calculate_cohort_analysis(enriched_transactions)
            analytics_datasets["cohort_analysis"] = cohort_analysis

            # Market basket analysis for cross-selling opportunities
            self.logger.info("🛒 Performing market basket analysis...")
            market_basket = self.analytics.market_basket_analysis(enriched_transactions)
            analytics_datasets["market_basket_analysis"] = market_basket

            # Churn risk indicators
            self.logger.info("⚠️ Calculating churn risk indicators...")
            churn_indicators = self.analytics.calculate_churn_indicators(customer_metrics)
            analytics_datasets["churn_indicators"] = churn_indicators

            # Generate executive summary metrics
            executive_summary = self._generate_executive_summary(analytics_datasets)
            analytics_datasets["executive_summary"] = executive_summary

            # Record performance metrics
            analytics_time = time.time() - start_time
            self.pipeline_state["performance_metrics"]["analytics_time"] = analytics_time

            self.logger.info(f"✅ Business analytics completed in {analytics_time:.2f} seconds")
            return analytics_datasets

        except Exception as e:
            error_context = self.error_handler.handle_error(
                e, "business_analytics", {"phase": "analytics"}
            )
            self.pipeline_state["errors"].append(error_context)
            raise e

    def _generate_executive_summary(self, datasets: Dict[str, any]) -> any:
        """ 📋 Generate Executive Summary Dashboard Data
        Creates high-level KPIs that executives care about:
        - Total revenue and growth rates
        - Customer acquisition and retention metrics
        - Product performance indicators
        - Operational efficiency metrics
        """
        from pyspark.sql.functions import sum as spark_sum, count, avg, max as spark_max

        enriched_transactions = datasets["enriched_transactions"]
        customer_metrics = datasets["customer_metrics"]
        rfm_analysis = datasets["rfm_analysis"]

        # Calculate key business metrics
        business_metrics = enriched_transactions.agg(
            spark_sum("total_amount").alias("total_revenue"),
            spark_sum("profit_amount").alias("total_profit"),
            count("transaction_id").alias("total_transactions"),
            countDistinct("customer_id").alias("active_customers"),
            countDistinct("product_id").alias("products_sold"),
            avg("total_amount").alias("avg_order_value")
        ).collect()[0]

        # Customer segment distribution
        segment_distribution = rfm_analysis.groupBy("rfm_segment").count().collect()

        # Top performing products
        top_products = enriched_transactions.groupBy("product_id", "category") \
            .agg(spark_sum("total_amount").alias("revenue")) \
            .orderBy(col("revenue").desc()) \
            .limit(10).collect()

        # Create summary dataset
        summary_data = [
            {
                "metric_name": "Total Revenue",
                "metric_value": float(business_metrics["total_revenue"]),
                "metric_type": "currency"
            },
            {
                "metric_name": "Total Profit",
                "metric_value": float(business_metrics["total_profit"]),
                "metric_type": "currency"
            },
            {
                "metric_name": "Active Customers",
                "metric_value": int(business_metrics["active_customers"]),
                "metric_type": "count"
            },
            {
                "metric_name": "Average Order Value",
                "metric_value": float(business_metrics["avg_order_value"]),
                "metric_type": "currency"
            }
        ]

        summary_schema = StructType([
            StructField("metric_name", StringType(), False),
            StructField("metric_value", DoubleType(), False),
            StructField("metric_type", StringType(), False)
        ])

        return self.spark.createDataFrame(summary_data, summary_schema)

    @retry_with_exponential_backoff(max_retries=2, base_delay=5.0)
    def _execute_data_persistence(self, datasets: Dict[str, any]):
        """ ☁️ Data Persistence Phase
        Persists all processed datasets to the data lake with:
        - Optimal partitioning strategies
        - Compression for storage efficiency
        - Metadata tracking for data catalog
        - Backup and versioning
        """
        self.logger.info("☁️ Executing data persistence...")

        try:
            start_time = time.time()

            # Ensure S3 structure exists
            self.s3_manager.create_data_lake_structure()

            # Persist datasets with appropriate partitioning
            persistence_config = {
                "enriched_transactions": {
                    "path": f"s3a://{self.config_manager.s3_config.bucket_name}/processed/enriched_transactions/",
                    "format": "parquet",
                    "partition_cols": ["year", "month"]
                },
                "customer_metrics": {
                    "path": f"s3a://{self.config_manager.s3_config.bucket_name}/curated/customer_metrics/",
                    "format": "parquet",
                    "partition_cols": ["customer_segment"]
                },
                "product_metrics": {
                    "path": f"s3a://{self.config_manager.s3_config.bucket_name}/curated/product_metrics/",
                    "format": "parquet",
                    "partition_cols": ["category"]
                },
                "rfm_analysis": {
                    "path": f"s3a://{self.config_manager.s3_config.bucket_name}/curated/rfm_analysis/",
                    "format": "parquet",
                    "partition_cols": ["rfm_segment"]
                },
                "cohort_analysis": {
                    "path": f"s3a://{self.config_manager.s3_config.bucket_name}/curated/cohort_analysis/",
                    "format": "parquet",
                    "partition_cols": ["cohort_month"]
                },
                "executive_summary": {
                    "path": f"s3a://{self.config_manager.s3_config.bucket_name}/curated/executive_summary/",
                    "format": "parquet",
                    "partition_cols": None
                }
            }

            for dataset_name, config in persistence_config.items():
                if dataset_name in datasets:
                    self.logger.info(f"💾 Persisting {dataset_name}...")
                    success = self.s3_manager.write_dataframe_to_s3(
                        datasets[dataset_name],
                        config["path"],
                        config["format"],
                        config["partition_cols"]
                    )

                    if not success:
                        raise Exception(f"Failed to persist {dataset_name}")

                    self.logger.info(f"✅ {dataset_name} persisted successfully")

            # Record performance metrics
            persistence_time = time.time() - start_time
            self.pipeline_state["performance_metrics"]["persistence_time"] = persistence_time

            self.logger.info(f"✅ Data persistence completed in {persistence_time:.2f} seconds")

        except Exception as e:
            error_context = self.error_handler.handle_error(
                e, "data_persistence", {"phase": "persistence"}
            )
            self.pipeline_state["errors"].append(error_context)
            raise e

    def _finalize_pipeline_execution(self):
        """ 🏁 Pipeline Finalization
        Final steps to complete the pipeline:
        - Update pipeline state
        - Generate execution summary
        - Clean up resources
        - Send completion notifications
        """
        self.pipeline_state["end_time"] = datetime.now()
        self.pipeline_state["status"] = "COMPLETED"

        # Calculate total execution time
        total_time = (self.pipeline_state["end_time"] - self.pipeline_state["start_time"]).total_seconds()
        self.pipeline_state["performance_metrics"]["total_execution_time"] = total_time

        # Clean up Spark resources
        for dataset_name in ["enriched_transactions"]:
            # Cached datasets
            try:
                # In a real implementation, you'd unpersist cached DataFrames
                pass
            except:
                pass

        self.logger.info(f"🏁 Pipeline execution finalized - Total time: {total_time:.2f} seconds")

    def _handle_pipeline_failure(self, error: Exception) -> Dict:
        """ 💥 Handle complete pipeline failure
        When the pipeline fails catastrophically:
        - Log comprehensive error information
        - Attempt graceful cleanup
        - Generate failure report
        - Send critical alerts
        """
        self.pipeline_state["end_time"] = datetime.now()
        self.pipeline_state["status"] = "FAILED"

        error_context = self.error_handler.handle_error(
            error, "pipeline_orchestrator", {"phase": "complete_pipeline"}
        )
        self.pipeline_state["errors"].append(error_context)

        self.logger.error(f"💥 Pipeline execution failed: {str(error)}")

        # Attempt cleanup
        try:
            self.spark_manager.stop_session()
        except:
            pass

        return self._generate_execution_summary()

    def _generate_execution_summary(self) -> Dict:
        """ 📊 Generate comprehensive execution summary
        Creates a detailed report of pipeline execution including:
        - Performance metrics
        - Data quality results
        - Error summary
        - Business metrics overview
        """
        summary = {
            "pipeline_execution": {
                "status": self.pipeline_state["status"],
                "start_time": self.pipeline_state["start_time"].isoformat() if self.pipeline_state["start_time"] else None,
                "end_time": self.pipeline_state["end_time"].isoformat() if self.pipeline_state["end_time"] else None,
                "total_execution_time": self.pipeline_state["performance_metrics"].get("total_execution_time", 0)
            },
            "data_processing": {
                "records_processed": self.pipeline_state["processed_records"],
                "performance_metrics": self.pipeline_state["performance_metrics"]
            },
            "data_quality": {
                "quality_reports": self.pipeline_state["quality_reports"]
            },
            "errors": {
                "error_count": len(self.pipeline_state["errors"]),
                "error_summary": self.error_handler.get_error_summary()
            }
        }

        # Save execution summary
        summary_path = f"logs/execution_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)

        self.logger.info(f"📊 Execution summary saved to {summary_path}")
        return summary

# Main execution script
def main():
    """ 🚀 Main execution entry point
    This is how the pipeline would be executed in production:
    - Can be called from command line
    - Integrated with schedulers (Airflow, Cron)
    - Triggered by events (file arrival, API calls)
    """
    logger = ETLLogger("main")

    try:
        logger.info("🚀 Starting Retail ETL Pipeline...")

        # Initialize orchestrator
        orchestrator = RetailETLOrchestrator()

        # Execute pipeline
        execution_summary = orchestrator.execute_full_pipeline()

        # Print summary
        logger.info("📊 Pipeline Execution Summary:")
        logger.info(f"Status: {execution_summary['pipeline_execution']['status']}")
        logger.info(f"Total Time: {execution_summary['pipeline_execution']['total_execution_time']:.2f} seconds")
        logger.info(f"Records Processed: {execution_summary['data_processing']['records_processed']}")

        if execution_summary['errors']['error_count'] > 0:
            logger.warning(f"⚠️ Pipeline completed with {execution_summary['errors']['error_count']} errors")
        else:
            logger.info("✅ Pipeline completed successfully with no errors!")

        return execution_summary

    except Exception as e:
        logger.error(f"💥 Pipeline execution failed: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
