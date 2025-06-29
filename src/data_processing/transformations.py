# src/data_processing/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from src.utils.logger import ETLLogger
from typing import Dict, List, Optional
import math

class RetailDataTransformer:
    """ 🏭 The transformation factory - where raw data becomes business gold!
    This class handles all the complex business logic transformations that convert raw retail data into actionable insights.
    Think of it as the brain of our ETL pipeline.
    """

    def __init__(self, spark_session):
        self.spark = spark_session
        self.logger = ETLLogger(__name__)

        # 📊 These window specifications are reusable patterns for analytics
        # They define how we partition and order data for calculations
        self.customer_window = Window.partitionBy("customer_id").orderBy("transaction_date")
        self.product_window = Window.partitionBy("product_id").orderBy("transaction_date")
        self.monthly_window = Window.partitionBy("year", "month").orderBy("transaction_date")

    def create_enriched_transactions(self, transactions_df: DataFrame, customers_df: DataFrame, products_df: DataFrame) -> DataFrame:
        """ 🎯 The Master Join Operation
        This is where the magic happens! We're creating a 360-degree view of each transaction by combining data from multiple sources.
        It's like assembling a puzzle where each piece adds crucial context to understand customer behavior.

        Why this matters:
        - Enables cross-dimensional analysis (customer + product + time)
        - Creates the foundation for all downstream analytics
        - Reduces the need for multiple joins in reporting queries
        """
        self.logger.info("🔗 Creating enriched transaction dataset...")

        # Step 1: Start with transactions as the base (fact table)
        enriched_df = transactions_df.filter(col("status") == "Completed")

        # Step 2: Add customer context - WHO is buying?
        # Left join ensures we keep all transactions even if customer data is missing
        enriched_df = enriched_df.join(
            customers_df.select(
                "customer_id",
                "customer_segment",
                "country",
                "age_group",
                "registration_date"
            ),
            "customer_id",
            "left"
        )

        # Step 3: Add product context - WHAT are they buying?
        enriched_df = enriched_df.join(
            products_df.select(
                "product_id",
                "category",
                "subcategory",
                "brand",
                "cost"
            ),
            "product_id",
            "left"
        )

        # Step 4: Add time-based dimensions for temporal analysis
        enriched_df = enriched_df.withColumn("year", year(col("transaction_date"))) \
            .withColumn("month", month(col("transaction_date"))) \
            .withColumn("quarter", quarter(col("transaction_date"))) \
            .withColumn("day_of_week", dayofweek(col("transaction_date"))) \
            .withColumn("hour", hour(col("transaction_date")))

        # Step 5: Calculate business metrics at transaction level
        enriched_df = enriched_df.withColumn(
            "profit_amount",
            col("total_amount") - (col("quantity") * col("cost"))
        ).withColumn(
            "profit_margin",
            when(col("total_amount") > 0,
                (col("profit_amount") / col("total_amount")) * 100
            ).otherwise(0)
        )

        # Step 6: Add customer tenure (how long they've been with us)
        enriched_df = enriched_df.withColumn(
            "customer_tenure_days",
            datediff(col("transaction_date"), col("registration_date"))
        )

        record_count = enriched_df.count()
        self.logger.info(f"✅ Created enriched dataset with {record_count:,} transactions")

        return enriched_df

    def calculate_customer_lifetime_metrics(self, enriched_df: DataFrame) -> DataFrame:
        """ 💎 Customer Lifetime Value (CLV) Calculation
        This is one of the most important metrics in retail! CLV helps us understand:
        - Which customers are most valuable
        - How much we can spend on customer acquisition
        - Which customer segments to focus on

        We're using a sophisticated approach that considers:
        - Recency: How recently did they purchase?
        - Frequency: How often do they purchase?
        - Monetary: How much do they spend?
        """
        self.logger.info("💎 Calculating customer lifetime metrics...")

        # Calculate RFM metrics (Recency, Frequency, Monetary)
        current_date = self.spark.sql("SELECT current_date() as today").collect()[0]["today"]

        customer_metrics = enriched_df.groupBy("customer_id").agg(
            # Monetary metrics
            sum("total_amount").alias("total_spent"),
            avg("total_amount").alias("avg_order_value"),
            sum("profit_amount").alias("total_profit_generated"),

            # Frequency metrics
            count("transaction_id").alias("total_orders"),
            countDistinct("product_id").alias("unique_products_purchased"),
            countDistinct("category").alias("categories_explored"),

            # Recency metrics
            max("transaction_date").alias("last_purchase_date"),
            min("transaction_date").alias("first_purchase_date"),

            # Customer context
            first("customer_segment").alias("customer_segment"),
            first("country").alias("country"),
            first("age_group").alias("age_group")
        )

        # Calculate derived metrics
        customer_metrics = customer_metrics.withColumn(
            "days_since_last_purchase",
            datediff(lit(current_date), col("last_purchase_date"))
        ).withColumn(
            "customer_lifespan_days",
            datediff(col("last_purchase_date"), col("first_purchase_date"))
        ).withColumn(
            "purchase_frequency",
            when(col("customer_lifespan_days") > 0,
                col("total_orders") / (col("customer_lifespan_days") / 30.0)
            ).otherwise(col("total_orders"))
        )

        # Calculate CLV using a simplified formula
        # CLV = Average Order Value × Purchase Frequency × Customer Lifespan
        customer_metrics = customer_metrics.withColumn(
            "estimated_clv",
            col("avg_order_value") * col("purchase_frequency") * (col("customer_lifespan_days") / 365.0)
        )

        # Create customer segments based on CLV
        clv_percentiles = customer_metrics.select(
            expr("percentile_approx(estimated_clv, 0.8)").alias("p80"),
            expr("percentile_approx(estimated_clv, 0.6)").alias("p60"),
            expr("percentile_approx(estimated_clv, 0.4)").alias("p40")
        ).collect()[0]

        customer_metrics = customer_metrics.withColumn(
            "clv_segment",
            when(col("estimated_clv") >= clv_percentiles["p80"], "Champions")
            .when(col("estimated_clv") >= clv_percentiles["p60"], "Loyal Customers")
            .when(col("estimated_clv") >= clv_percentiles["p40"], "Potential Loyalists")
            .otherwise("New Customers")
        )

        self.logger.info("✅ Customer lifetime metrics calculated successfully")
        return customer_metrics

    def create_product_performance_metrics(self, enriched_df: DataFrame) -> DataFrame:
        """ 📈 Product Performance Analytics
        Understanding which products drive your business is crucial for:
        - Inventory management
        - Marketing focus
        - Pricing strategies
        - Product development decisions

        This function creates comprehensive product analytics that answer:
        - Which products are bestsellers?
        - What's the profit margin by product?
        - How do products perform across different customer segments?
        """
        self.logger.info("📈 Calculating product performance metrics...")

        # Basic product metrics
        product_metrics = enriched_df.groupBy(
            "product_id", "category", "subcategory", "brand"
        ).agg(
            # Sales metrics
            sum("quantity").alias("total_units_sold"),
            sum("total_amount").alias("total_revenue"),
            sum("profit_amount").alias("total_profit"),
            count("transaction_id").alias("total_transactions"),

            # Customer metrics
            countDistinct("customer_id").alias("unique_customers"),

            # Performance metrics
            avg("total_amount").alias("avg_transaction_value"),
            avg("profit_margin").alias("avg_profit_margin")
        )

        # Calculate advanced metrics
        product_metrics = product_metrics.withColumn(
            "revenue_per_customer",
            col("total_revenue") / col("unique_customers")
        ).withColumn(
            "units_per_transaction",
            col("total_units_sold") / col("total_transactions")
        )

        # Add ranking within categories
        category_window = Window.partitionBy("category").orderBy(col("total_revenue").desc())

        product_metrics = product_metrics.withColumn(
            "category_revenue_rank",
            row_number().over(category_window)
        ).withColumn(
            "is_top_performer",
            when(col("category_revenue_rank") <= 5, True).otherwise(False)
        )

        self.logger.info("✅ Product performance metrics calculated")
        return product_metrics

    def create_time_series_analytics(self, enriched_df: DataFrame) -> DataFrame:
        """ 📅 Time Series Analytics for Trend Analysis
        Time-based analysis is crucial for:
        - Identifying seasonal patterns
        - Forecasting future sales
        - Understanding business cycles
        - Planning inventory and marketing campaigns

        This creates daily, weekly, and monthly aggregations with trend indicators.
        """
        self.logger.info("📅 Creating time series analytics...")

        # Daily aggregations
        daily_metrics = enriched_df.groupBy("year", "month", "day_of_week").agg(
            sum("total_amount").alias("daily_revenue"),
            sum("profit_amount").alias("daily_profit"),
            count("transaction_id").alias("daily_transactions"),
            countDistinct("customer_id").alias("daily_unique_customers"),
            avg("total_amount").alias("daily_avg_order_value")
        )

        # Add day-over-day growth calculations
        daily_window = Window.partitionBy("day_of_week").orderBy("year", "month")
        daily_metrics = daily_metrics.withColumn(
            "revenue_growth_rate",
            ((col("daily_revenue") - lag("daily_revenue", 1).over(daily_window)) / lag("daily_revenue", 1).over(daily_window)) * 100
        )

        # Monthly aggregations for executive reporting
        monthly_metrics = enriched_df.groupBy("year", "month").agg(
            sum("total_amount").alias("monthly_revenue"),
            sum("profit_amount").alias("monthly_profit"),
            count("transaction_id").alias("monthly_transactions"),
            countDistinct("customer_id").alias("monthly_active_customers"),
            countDistinct("product_id").alias("monthly_products_sold")
        )

        # Calculate month-over-month growth
        monthly_window = Window.orderBy("year", "month")
        monthly_metrics = monthly_metrics.withColumn(
            "mom_revenue_growth",
            ((col("monthly_revenue") - lag("monthly_revenue", 1).over(monthly_window)) / lag("monthly_revenue", 1).over(monthly_window)) * 100
        ).withColumn(
            "mom_customer_growth",
            ((col("monthly_active_customers") - lag("monthly_active_customers", 1).over(monthly_window)) / lag("monthly_active_customers", 1).over(monthly_window)) * 100
        )

        self.logger.info("✅ Time series analytics completed")
        return daily_metrics, monthly_metrics
