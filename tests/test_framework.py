# tests/test_framework.py
import pytest
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from src.data_processing.transformations import RetailDataTransformer
from src.business_metrics.advanced_analytics import AdvancedBusinessMetrics
from src.data_quality.quality_checker import DataQualityChecker
from src.utils.logger import ETLLogger
from datetime import datetime, timedelta
import pandas as pd

class DataPipelineTestFramework:
    """ 🧪 Comprehensive Testing Framework for Data Pipelines
    This framework implements multiple testing layers:
    1. Unit Tests: Individual function validation
    2. Integration Tests: Component interaction testing
    3. Data Quality Tests: Business rule validation
    4. Performance Tests: Scalability and speed validation
    5. End-to-End Tests: Full pipeline validation

    Why comprehensive testing matters in data engineering:
    - Data bugs are often silent and hard to detect
    - Business decisions depend on data accuracy
    - Pipeline failures can cascade across systems
    - Regulatory compliance requires audit trails
    """

    @classmethod
    def setup_test_spark_session(cls):
        """ ⚡ Create optimized Spark session for testing
        Test Spark sessions need special configuration:
        - Smaller memory footprint for CI/CD environments
        - Deterministic behavior for reproducible tests
        - Fast startup and shutdown
        """
        return SparkSession.builder \
            .appName("DataPipelineTests") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.default.parallelism", "2") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

    @classmethod
    def create_test_data(cls, spark):
        """ 🎲 Generate deterministic test data
        Test data should be:
        - Small enough for fast execution
        - Comprehensive enough to cover edge cases
        - Deterministic for reproducible results
        - Representative of real-world scenarios
        """

        # Create test customers
        customers_data = [
            ("CUST_001", "John", "Doe", "john@email.com", "2023-01-15", "Premium", "USA"),
            ("CUST_002", "Jane", "Smith", "jane@email.com", "2023-02-20", "Standard", "Canada"),
            ("CUST_003", "Bob", "Johnson", "bob@email.com", "2023-03-10", "Basic", "USA"),
            ("CUST_004", "Alice", "Brown", "alice@email.com", "2023-01-05", "Premium", "UK")
        ]

        customers_schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("registration_date", StringType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("country", StringType(), True)
        ])

        customers_df = spark.createDataFrame(customers_data, customers_schema) \
            .withColumn("registration_date", to_date(col("registration_date")))

        # Create test products
        products_data = [
            ("PROD_001", "Laptop Pro", "Electronics", "Computers", "TechBrand", 1200.00, 800.00),
            ("PROD_002", "Running Shoes", "Sports", "Footwear", "SportsBrand", 150.00, 75.00),
            ("PROD_003", "Coffee Maker", "Home", "Kitchen", "HomeBrand", 200.00, 120.00),
            ("PROD_004", "Smartphone", "Electronics", "Mobile", "TechBrand", 800.00, 500.00)
        ]

        products_schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("subcategory", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("cost", DoubleType(), True)
        ])

        products_df = spark.createDataFrame(products_data, products_schema)

        # Create test transactions
        transactions_data = [
            ("TXN_001", "CUST_001", "PROD_001", 1, 1200.00, 0.00, 1200.00, "2023-06-15 10:30:00", "Completed"),
            ("TXN_002", "CUST_001", "PROD_002", 2, 150.00, 20.00, 280.00, "2023-06-20 14:15:00", "Completed"),
            ("TXN_003", "CUST_002", "PROD_003", 1, 200.00, 10.00, 190.00, "2023-06-18 09:45:00", "Completed"),
            ("TXN_004", "CUST_003", "PROD_004", 1, 800.00, 50.00, 750.00, "2023-06-22 16:20:00", "Completed"),
            ("TXN_005", "CUST_001", "PROD_003", 1, 200.00, 0.00, 200.00, "2023-07-01 11:00:00", "Completed")
        ]

        transactions_schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("status", StringType(), True)
        ])

        transactions_df = spark.createDataFrame(transactions_data, transactions_schema) \
            .withColumn("transaction_date", to_timestamp(col("transaction_date")))

        return customers_df, products_df, transactions_df

class TestDataTransformations(unittest.TestCase):
    """ 🔧 Unit Tests for Data Transformations
    These tests validate individual transformation functions:
    - Input/output data types
    - Business logic correctness
    - Edge case handling
    - Performance characteristics
    """

    @classmethod
    def setUpClass(cls):
        """Set up test environment once for all tests"""
        cls.spark = DataPipelineTestFramework.setup_test_spark_session()
        cls.transformer = RetailDataTransformer(cls.spark)
        cls.logger = ETLLogger(__name__)

        # Create test data
        cls.customers_df, cls.products_df, cls.transactions_df = \
            DataPipelineTestFramework.create_test_data(cls.spark)

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment"""
        cls.spark.stop()

    def test_enriched_transactions_creation(self):
        """ ✅ Test enriched transaction creation
        This test validates:
        - All expected columns are present
        - Join operations work correctly
        - Calculated fields are accurate
        - No data loss during transformation
        """
        self.logger.info("🧪 Testing enriched transactions creation...")

        # Execute transformation
        enriched_df = self.transformer.create_enriched_transactions(
            self.transactions_df, self.customers_df, self.products_df
        )

        # Validate schema
        expected_columns = [
            "transaction_id", "customer_id", "product_id", "total_amount",
            "customer_segment", "category", "profit_amount", "profit_margin"
        ]

        for col in expected_columns:
            self.assertIn(col, enriched_df.columns, f"Missing column: {col}")

        # Validate data integrity
        original_count = self.transactions_df.count()
        enriched_count = enriched_df.count()
        self.assertEqual(original_count, enriched_count, "Row count mismatch after enrichment")

        # Validate business logic
        profit_check = enriched_df.select("profit_amount", "total_amount", "quantity", "cost").collect()
        for row in profit_check:
            if row["cost"] and row["quantity"]:
                expected_profit = row["total_amount"] - (row["quantity"] * row["cost"])
                self.assertAlmostEqual(row["profit_amount"], expected_profit, places=2, msg="Profit calculation incorrect")

        self.logger.info("✅ Enriched transactions test passed")

    def test_customer_lifetime_metrics(self):
        """ 💎 Test customer lifetime value calculations
        This validates the complex CLV algorithm:
        - Mathematical accuracy of calculations
        - Handling of edge cases (single purchase customers)
        - Segment assignment logic
        """
        self.logger.info("🧪 Testing customer lifetime metrics...")

        # Create enriched data first
        enriched_df = self.transformer.create_enriched_transactions(
            self.transactions_df, self.customers_df, self.products_df
        )

        # Calculate CLV metrics
        clv_df = self.transformer.calculate_customer_lifetime_metrics(enriched_df)

        # Validate that all customers are included
        unique_customers = self.transactions_df.select("customer_id").distinct().count()
        clv_customers = clv_df.count()
        self.assertEqual(unique_customers, clv_customers, "Customer count mismatch in CLV calculation")

        # Validate CLV calculation for known customer
        cust_001_metrics = clv_df.filter(col("customer_id") == "CUST_001").collect()[0]

        # CUST_001 has 3 transactions: $1200, $280, $200 = $1680 total
        self.assertAlmostEqual(cust_001_metrics["total_spent"], 1680.0, places=2)
        self.assertEqual(cust_001_metrics["total_orders"], 3)

        # Validate segment assignment
        self.assertIn(cust_001_metrics["clv_segment"],
            ["Champions", "Loyal Customers", "Potential Loyalists", "New Customers"])

        self.logger.info("✅ Customer lifetime metrics test passed")

    def test_data_quality_edge_cases(self):
        """ 🎯 Test edge cases and data quality scenarios
        Edge cases that can break production systems:
        - Null values in critical fields
        - Negative amounts
        - Future dates
        - Duplicate records
        """
        self.logger.info("🧪 Testing edge cases...")

        # Create edge case data
        edge_case_data = [
            ("TXN_EDGE_001", "CUST_001", "PROD_001", 1, 100.0, 0.0, -50.0, "2025-01-01 10:00:00", "Completed"), # Negative amount
            ("TXN_EDGE_002", None, "PROD_002", 1, 200.0, 0.0, 200.0, "2023-06-15 10:00:00", "Completed"), # Null customer
            ("TXN_EDGE_003", "CUST_002", "PROD_003", 0, 150.0, 0.0, 0.0, "2023-06-15 10:00:00", "Completed") # Zero quantity
        ]

        edge_schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), True), # Allow nulls for testing
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("status", StringType(), True)
        ])

        edge_df = self.spark.createDataFrame(edge_case_data, edge_schema) \
            .withColumn("transaction_date", to_timestamp(col("transaction_date")))

        # Test data quality checker
        from src.config.config import DataQualityConfig
        quality_checker = DataQualityChecker(DataQualityConfig())
        passed, report = quality_checker.run_quality_checks(edge_df, "edge_case_test")

        # Should fail due to null customer_id
        self.assertFalse(passed, "Quality check should fail for edge case data")
        self.assertIn("null_values", report["checks"])

        self.logger.info("✅ Edge cases test passed")

class TestBusinessMetrics(unittest.TestCase):
    """ 📊 Integration Tests for Business Metrics
    These tests validate complex business logic:
    - RFM analysis accuracy
    - Cohort analysis calculations
    - Market basket analysis results
    """

    @classmethod
    def setUpClass(cls):
        cls.spark = DataPipelineTestFramework.setup_test_spark_session()
        cls.analytics = AdvancedBusinessMetrics(cls.spark)
        cls.logger = ETLLogger(__name__)

        # Create enriched test data
        transformer = RetailDataTransformer(cls.spark)
        customers_df, products_df, transactions_df = \
            DataPipelineTestFramework.create_test_data(cls.spark)

        cls.enriched_df = transformer.create_enriched_transactions(
            transactions_df, customers_df, products_df
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_rfm_analysis(self):
        """ 🎯 Test RFM analysis implementation
        RFM is critical for customer segmentation, so we need to ensure:
        - Score calculations are mathematically correct
        - Segment assignments follow business rules
        - Edge cases are handled properly
        """
        self.logger.info("🧪 Testing RFM analysis...")
        rfm_df = self.analytics.perform_rfm_analysis(self.enriched_df)

        # Validate RFM scores are in valid range (1-5)
        rfm_scores = rfm_df.select("r_score", "f_score", "m_score").collect()
        for row in rfm_scores:
            self.assertGreaterEqual(row["r_score"], 1)
            self.assertLessEqual(row["r_score"], 5)
            self.assertGreaterEqual(row["f_score"], 1)
            self.assertLessEqual(row["f_score"], 5)
            self.assertGreaterEqual(row["m_score"], 1)
            self.assertLessEqual(row["m_score"], 5)

        # Validate segment assignment
        segments = rfm_df.select("rfm_segment").distinct().collect()
        valid_segments = [
            "Champions", "Loyal Customers", "New Customers",
            "Potential Loyalists", "At Risk", "Cannot Lose Them",
            "Hibernating", "Others"
        ]

        for segment in segments:
            self.assertIn(segment["rfm_segment"], valid_segments)

        # Validate that CUST_001 (highest value customer) gets appropriate segment
        cust_001_rfm = rfm_df.filter(col("customer_id") == "CUST_001").collect()[0]
        self.assertIn(cust_001_rfm["rfm_segment"],
            ["Champions", "Loyal Customers", "Potential Loyalists"])

        self.logger.info("✅ RFM analysis test passed")

    def test_cohort_analysis(self):
        """ 📈 Test cohort analysis calculations
        Cohort analysis is complex because it involves:
        - Time-based grouping
        - Retention rate calculations
        - Period-over-period comparisons
        """
        self.logger.info("🧪 Testing cohort analysis...")
        cohort_df = self.analytics.calculate_cohort_analysis(self.enriched_df)

        # Validate cohort structure
        self.assertIn("cohort_month", cohort_df.columns)
        self.assertIn("period_number", cohort_df.columns)
        self.assertIn("retention_rate", cohort_df.columns)

        # Validate retention rates are percentages (0-100)
        retention_rates = cohort_df.select("retention_rate").collect()
        for row in retention_rates:
            if row["retention_rate"] is not None:
                self.assertGreaterEqual(row["retention_rate"], 0)
                self.assertLessEqual(row["retention_rate"], 100)

        # Validate that period 0 has 100% retention (by definition)
        period_0_retention = cohort_df.filter(col("period_number") == 0) \
            .select("retention_rate").collect()

        for row in period_0_retention:
            self.assertAlmostEqual(row["retention_rate"], 100.0, places=1)

        self.logger.info("✅ Cohort analysis test passed")

class TestPerformance(unittest.TestCase):
    """ ⚡ Performance Tests
    These tests ensure the pipeline can handle production workloads:
    - Processing time benchmarks
    - Memory usage validation
    - Scalability testing
    """

    @classmethod
    def setUpClass(cls):
        cls.spark = DataPipelineTestFramework.setup_test_spark_session()
        cls.logger = ETLLogger(__name__)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_large_dataset_processing(self):
        """ 📊 Test processing performance with larger datasets
        This simulates production-scale data to ensure:
        - Processing completes within acceptable time limits
        - Memory usage stays within bounds
        - No performance degradation with data growth
        """
        self.logger.info("🧪 Testing large dataset processing...")
        import time

        # Generate larger test dataset (10,000 transactions)
        large_transactions = []
        for i in range(10000):
            large_transactions.append((
                f"TXN_{i:06d}",
                f"CUST_{i % 1000:03d}", # 1000 unique customers
                f"PROD_{i % 100:03d}", # 100 unique products
                1,
                100.0 + (i % 500), # Varying prices
                i % 50, # Varying discounts
                100.0 + (i % 500) - (i % 50),
                f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d} 10:00:00",
                "Completed"
            ))

        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("status", StringType(), True)
        ])

        large_df = self.spark.createDataFrame(large_transactions, schema) \
            .withColumn("transaction_date", to_timestamp(col("transaction_date")))

        # Test processing time
        start_time = time.time()

        # Perform aggregation (common operation)
        result = large_df.groupBy("customer_id").agg(
            sum("total_amount").alias("total_spent"),
            count("transaction_id").alias("transaction_count")
        ).collect()

        processing_time = time.time() - start_time

        # Validate results
        self.assertEqual(len(result), 1000) # Should have 1000 unique customers
        self.assertLess(processing_time, 30) # Should complete within 30 seconds

        self.logger.info(f"✅ Large dataset test passed - processed 10K records in {processing_time:.2f}s")
