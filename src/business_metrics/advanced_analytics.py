# src/business_metrics/advanced_analytics.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from src.utils.logger import ETLLogger
from typing import Tuple, Dict
import numpy as np

class AdvancedBusinessMetrics:
    """ 🎯 Advanced Analytics Engine
    This class implements sophisticated business metrics that provide deep insights:
    - Customer segmentation using RFM analysis
    - Cohort analysis for retention tracking
    - Market basket analysis for cross-selling
    - Churn prediction indicators

    These metrics help answer questions like:
    - Which customers are at risk of churning?
    - What products are frequently bought together?
    - How do customer cohorts perform over time?
    """

    def __init__(self, spark_session):
        self.spark = spark_session
        self.logger = ETLLogger(__name__)

    def perform_rfm_analysis(self, enriched_df: DataFrame) -> DataFrame:
        """ 🎯 RFM Analysis - The Gold Standard of Customer Segmentation
        RFM stands for Recency, Frequency, Monetary - three key dimensions that predict customer behavior better than demographics alone!

        Why RFM works:
        - Recency: Recent customers are more likely to respond to offers
        - Frequency: Frequent customers show loyalty and engagement
        - Monetary: High-value customers drive profitability

        This creates actionable customer segments for targeted marketing.
        """
        self.logger.info("🎯 Performing RFM analysis...")

        # Calculate the reference date (latest transaction date)
        max_date = enriched_df.agg(max("transaction_date")).collect()[0][0]

        # Calculate RFM metrics for each customer
        rfm_data = enriched_df.groupBy("customer_id").agg(
            # Recency: Days since last purchase
            datediff(lit(max_date), max("transaction_date")).alias("recency"),
            # Frequency: Number of transactions
            count("transaction_id").alias("frequency"),
            # Monetary: Total amount spent
            sum("total_amount").alias("monetary")
        )

        # Calculate quintiles for each RFM dimension
        # Quintiles divide customers into 5 equal groups (1=worst, 5=best)
        rfm_quintiles = rfm_data.select(
            expr("percentile_approx(recency, array(0.2, 0.4, 0.6, 0.8))").alias("recency_quintiles"),
            expr("percentile_approx(frequency, array(0.2, 0.4, 0.6, 0.8))").alias("frequency_quintiles"),
            expr("percentile_approx(monetary, array(0.2, 0.4, 0.6, 0.8))").alias("monetary_quintiles")
        ).collect()[0]

        # Assign RFM scores (1-5 scale)
        # Note: For recency, lower values are better (more recent), so we reverse the scoring
        rfm_scored = rfm_data.withColumn(
            "r_score",
            when(col("recency") <= rfm_quintiles["recency_quintiles"][0], 5)
            .when(col("recency") <= rfm_quintiles["recency_quintiles"][1], 4)
            .when(col("recency") <= rfm_quintiles["recency_quintiles"][2], 3)
            .when(col("recency") <= rfm_quintiles["recency_quintiles"][3], 2)
            .otherwise(1)
        ).withColumn(
            "f_score",
            when(col("frequency") >= rfm_quintiles["frequency_quintiles"][3], 5)
            .when(col("frequency") >= rfm_quintiles["frequency_quintiles"][2], 4)
            .when(col("frequency") >= rfm_quintiles["frequency_quintiles"][1], 3)
            .when(col("frequency") >= rfm_quintiles["frequency_quintiles"][0], 2)
            .otherwise(1)
        ).withColumn(
            "m_score",
            when(col("monetary") >= rfm_quintiles["monetary_quintiles"][3], 5)
            .when(col("monetary") >= rfm_quintiles["monetary_quintiles"][2], 4)
            .when(col("monetary") >= rfm_quintiles["monetary_quintiles"][1], 3)
            .when(col("monetary") >= rfm_quintiles["monetary_quintiles"][0], 2)
            .otherwise(1)
        )

        # Create RFM segments based on scores
        rfm_segments = rfm_scored.withColumn(
            "rfm_segment",
            when((col("r_score") >= 4) & (col("f_score") >= 4) & (col("m_score") >= 4), "Champions")
            .when((col("r_score") >= 3) & (col("f_score") >= 3) & (col("m_score") >= 3), "Loyal Customers")
            .when((col("r_score") >= 4) & (col("f_score") <= 2), "New Customers")
            .when((col("r_score") >= 3) & (col("f_score") <= 2) & (col("m_score") >= 3), "Potential Loyalists")
            .when((col("r_score") <= 2) & (col("f_score") >= 3) & (col("m_score") >= 3), "At Risk")
            .when((col("r_score") <= 2) & (col("f_score") <= 2) & (col("m_score") >= 3), "Cannot Lose Them")
            .when((col("r_score") <= 2) & (col("f_score") <= 2) & (col("m_score") <= 2), "Hibernating")
            .otherwise("Others")
        ).withColumn(
            "rfm_score",
            concat(col("r_score"), col("f_score"), col("m_score"))
        )

        # Add business recommendations for each segment
        rfm_segments = rfm_segments.withColumn(
            "recommended_action",
            when(col("rfm_segment") == "Champions", "Reward them. They can become advocates.")
            .when(col("rfm_segment") == "Loyal Customers", "Upsell higher value products.")
            .when(col("rfm_segment") == "New Customers", "Provide onboarding support.")
            .when(col("rfm_segment") == "At Risk", "Send personalized reactivation campaigns.")
            .when(col("rfm_segment") == "Cannot Lose Them", "Win them back via renewals or newer products.")
            .otherwise("Re-engage with special offers.")
        )

        self.logger.info("✅ RFM analysis completed")
        return rfm_segments

    def calculate_cohort_analysis(self, enriched_df: DataFrame) -> DataFrame:
        """ 📈 Cohort Analysis - Understanding Customer Retention Over Time
        Cohort analysis tracks groups of customers over time to understand:
        - How customer behavior changes after acquisition
        - Which acquisition channels produce the best long-term customers
        - What the natural customer lifecycle looks like

        This is essential for:
        - Calculating accurate customer lifetime value
        - Optimizing marketing spend
        - Identifying retention issues early
        """
        self.logger.info("📈 Performing cohort analysis...")

        # Step 1: Identify each customer's first purchase (cohort assignment)
        customer_cohorts = enriched_df.groupBy("customer_id").agg(
            min("transaction_date").alias("first_purchase_date")
        ).withColumn(
            "cohort_month",
            date_format(col("first_purchase_date"), "yyyy-MM")
        )

        # Step 2: Join back to get cohort info for all transactions
        cohort_data = enriched_df.join(customer_cohorts, "customer_id")

        # Step 3: Calculate period number (months since first purchase)
        cohort_data = cohort_data.withColumn(
            "transaction_month",
            date_format(col("transaction_date"), "yyyy-MM")
        ).withColumn(
            "period_number",
            months_between(col("transaction_date"), col("first_purchase_date"))
        )

        # Step 4: Calculate cohort metrics
        cohort_metrics = cohort_data.groupBy("cohort_month", "period_number").agg(
            countDistinct("customer_id").alias("customers_in_period"),
            sum("total_amount").alias("revenue_in_period"),
            avg("total_amount").alias("avg_order_value_in_period")
        )

        # Step 5: Calculate cohort sizes (customers in month 0)
        cohort_sizes = cohort_metrics.filter(col("period_number") == 0) \
            .select("cohort_month",
            col("customers_in_period").alias("cohort_size"))

        # Step 6: Calculate retention rates
        cohort_retention = cohort_metrics.join(cohort_sizes, "cohort_month") \
            .withColumn(
                "retention_rate",
                (col("customers_in_period") / col("cohort_size")) * 100
            )

        self.logger.info("✅ Cohort analysis completed")
        return cohort_retention

    def market_basket_analysis(self, enriched_df: DataFrame, min_support: float = 0.01) -> DataFrame:
        """ 🛒 Market Basket Analysis - What Products Go Together?
        This analysis discovers which products are frequently bought together, enabling:
        - Cross-selling recommendations ("Customers who bought X also bought Y")
        - Store layout optimization
        - Bundle pricing strategies
        - Inventory planning

        We use the Apriori algorithm concept to find frequent itemsets.
        """
        self.logger.info("🛒 Performing market basket analysis...")

        # Step 1: Create transaction baskets (products bought together)
        # Group by customer and date to identify shopping sessions
        transaction_baskets = enriched_df.groupBy("customer_id", "transaction_date").agg(
            collect_list("product_id").alias("products_in_basket"),
            sum("total_amount").alias("basket_value")
        ).withColumn(
            "basket_size",
            size(col("products_in_basket"))
        )

        # Step 2: Find frequent product pairs
        # This is a simplified version - in production, you'd use MLlib's FPGrowth
        product_pairs = transaction_baskets.filter(col("basket_size") >= 2) \
            .select("customer_id", "transaction_date",
            explode(col("products_in_basket")).alias("product_1")) \
            .join(
                transaction_baskets.filter(col("basket_size") >= 2) \
                    .select("customer_id", "transaction_date",
                    explode(col("products_in_basket")).alias("product_2")),
                ["customer_id", "transaction_date"]
            ).filter(col("product_1") < col("product_2")) # Avoid duplicates

        # Step 3: Calculate support and confidence
        total_transactions = transaction_baskets.count()

        pair_counts = product_pairs.groupBy("product_1", "product_2").agg(
            count("*").alias("pair_count")
        ).withColumn(
            "support",
            col("pair_count") / total_transactions
        ).filter(col("support") >= min_support)

        # Step 4: Add product information for interpretability
        from src.data_processing.transformations import RetailDataTransformer

        # Get product details
        product_info = enriched_df.select("product_id", "category", "brand").distinct()

        market_basket_results = pair_counts.join(
            product_info.withColumnRenamed("product_id", "product_1")
            .withColumnRenamed("category", "category_1")
            .withColumnRenamed("brand", "brand_1"),
            "product_1"
        ).join(
            product_info.withColumnRenamed("product_id", "product_2")
            .withColumnRenamed("category", "category_2")
            .withColumnRenamed("brand", "brand_2"),
            "product_2"
        ).orderBy(col("support").desc())

        self.logger.info("✅ Market basket analysis completed")
        return market_basket_results

    def calculate_churn_indicators(self, customer_metrics_df: DataFrame) -> DataFrame:
        """ ⚠️ Churn Risk Prediction Indicators
        Identifying customers at risk of churning is crucial for retention efforts. This function creates early warning indicators based on:
        - Purchase recency patterns
        - Frequency changes
        - Spending behavior shifts

        These indicators help prioritize retention campaigns and interventions.
        """
        self.logger.info("⚠️ Calculating churn risk indicators...")

        # Define churn risk factors
        churn_indicators = customer_metrics_df.withColumn(
            "days_since_last_purchase_risk",
            when(col("days_since_last_purchase") > 90, "High")
            .when(col("days_since_last_purchase") > 60, "Medium")
            .when(col("days_since_last_purchase") > 30, "Low")
            .otherwise("Very Low")
        ).withColumn(
            "frequency_risk",
            when(col("total_orders") == 1, "High")
            .when(col("total_orders") <= 3, "Medium")
            .otherwise("Low")
        ).withColumn(
            "engagement_risk",
            when(col("categories_explored") == 1, "High")
            .when(col("categories_explored") <= 2, "Medium")
            .otherwise("Low")
        )

        # Calculate overall churn risk score
        churn_indicators = churn_indicators.withColumn(
            "churn_risk_score",
            when(col("days_since_last_purchase_risk") == "High", 3).otherwise(0) +
            when(col("frequency_risk") == "High", 2).otherwise(0) +
            when(col("engagement_risk") == "High", 1).otherwise(0)
        ).withColumn(
            "churn_risk_level",
            when(col("churn_risk_score") >= 4, "Critical")
            .when(col("churn_risk_score") >= 2, "High")
            .when(col("churn_risk_score") >= 1, "Medium")
            .otherwise("Low")
        )

        # Add recommended interventions
        churn_indicators = churn_indicators.withColumn(
            "recommended_intervention",
            when(col("churn_risk_level") == "Critical", "Immediate personal outreach + special offer")
            .when(col("churn_risk_level") == "High", "Targeted email campaign + discount")
            .when(col("churn_risk_level") == "Medium", "Newsletter + product recommendations")
            .otherwise("Standard marketing communications")
        )

        self.logger.info("✅ Churn risk indicators calculated")
        return churn_indicators
