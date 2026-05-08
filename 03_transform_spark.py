"""
03_transform_spark.py  (T — Transform)
----------------------------------------
يقرأ البيانات الخام من HDFS Bronze Layer
ويبنيها كـ Star Schema في HDFS Gold Layer.

Star Schema:
    fact_sales          ← الجدول الرئيسي (transactions)
    dim_customer        ← بيانات العميل
    dim_product         ← بيانات المنتج
    dim_date            ← بيانات التاريخ (calendar table)
    dim_region          ← بيانات المنطقة الجغرافية
    dim_payment         ← طريقة الدفع
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

os.environ["HADOOP_USER_NAME"] = "root"

# ─── Config ──────────────────────────────────────────────────────────────────
HDFS_BRONZE = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/sales/"
HDFS_GOLD   = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SalesETL_Transform")
        .master("yarn")
        .config("spark.hadoop.fs.defaultFS",                          "hdfs://hadoop-namenode:9000")
        .config("spark.hadoop.yarn.resourcemanager.hostname",         "resourcemanager")
        .config("spark.hadoop.yarn.resourcemanager.address",          "resourcemanager:8032")
        .config("spark.hadoop.yarn.resourcemanager.scheduler.address","resourcemanager:8030")
        .config("spark.driver.host",                                  "172.30.1.13")
        .config("spark.driver.bindAddress",                           "0.0.0.0")
        .config("spark.executor.memory",                              "512m")
        .config("spark.yarn.am.memory",                               "512m")
        .getOrCreate()
    )


def build_dim_customer(df):
    """
    dim_customer: بيانات العميل الفريدة.
    customer_id هو الـ primary key.
    """
    log.info("🔨 Building dim_customer ...")
    return (
        df.select("customer_id", "customer_age", "customer_gender")
        .dropDuplicates(["customer_id"])
        .withColumn("customer_age",
                    F.col("customer_age").cast(IntegerType()))
    )


def build_dim_product(df):
    """
    dim_product: بيانات المنتج والفئة.
    """
    log.info("🔨 Building dim_product ...")
    return (
        df.select("product_id", "category")
        .dropDuplicates(["product_id"])
    )


def build_dim_date(df):
    """
    dim_date: calendar table مشتقة من order_date.
    date_key = YYYYMMDD كـ integer (e.g. 20240115)
    """
    log.info("🔨 Building dim_date ...")
    df_dated = df.withColumn("order_date_parsed", F.to_date("order_date", "yyyy-MM-dd"))
    return (
        df_dated.select("order_date_parsed").distinct()
        .select(
            F.date_format("order_date_parsed", "yyyyMMdd")
             .cast("int").alias("date_key"),
            F.col("order_date_parsed").alias("full_date"),
            F.year("order_date_parsed").alias("year"),
            F.quarter("order_date_parsed").alias("quarter"),
            F.month("order_date_parsed").alias("month"),
            F.date_format("order_date_parsed", "MMMM").alias("month_name"),
            F.dayofmonth("order_date_parsed").alias("day"),
            F.date_format("order_date_parsed", "EEEE").alias("day_name"),
            F.weekofyear("order_date_parsed").alias("week_of_year"),
            F.when(F.dayofweek("order_date_parsed").isin(1, 7), True)
             .otherwise(False).alias("is_weekend"),
        )
        .dropDuplicates(["date_key"])
        .orderBy("date_key")
    )


def build_dim_region(df):
    """
    dim_region: المناطق الجغرافية.
    region_id = surrogate key مولّد تلقائياً.
    """
    log.info("🔨 Building dim_region ...")
    return (
        df.select(F.col("region").alias("region_name"))
        .dropDuplicates(["region_name"])
        .withColumn("region_id", F.monotonically_increasing_id())
        .select("region_id", "region_name")
    )


def build_dim_payment(df):
    """
    dim_payment: طرق الدفع.
    """
    log.info("🔨 Building dim_payment ...")
    return (
        df.select(F.col("payment_method"))
        .dropDuplicates(["payment_method"])
        .withColumn("payment_id", F.monotonically_increasing_id())
        .select("payment_id", "payment_method")
    )


def build_fact_sales(df, dim_region, dim_payment):
    """
    fact_sales: الـ fact table الرئيسية.
    كل row = transaction واحدة.
    بيحتوي على:
        - المقاييس (measures): total_amount, profit_margin, shipping_cost, etc.
        - Foreign Keys: date_key, region_id, payment_id
        - Natural Keys: order_id, customer_id, product_id
    """
    log.info("🔨 Building fact_sales ...")

    df_dated = df.withColumn(
        "date_key",
        F.date_format(F.to_date("order_date", "yyyy-MM-dd"), "yyyyMMdd").cast("int")
    )

    # Join مع dim_region للحصول على region_id
    df_with_region = df_dated.join(
        dim_region, on="region_name" if "region_name" in dim_region.columns else
                    (df_dated["region"] == dim_region["region_name"]),
        how="left"
    ) if "region_name" in dim_region.columns else (
        df_dated.join(
            dim_region.withColumnRenamed("region_name", "region"),
            on="region",
            how="left"
        )
    )

    # Join مع dim_payment
    df_full = df_with_region.join(dim_payment, on="payment_method", how="left")

    fact = df_full.select(
        "order_id",               # طبيعي ← للـ tracing
        "customer_id",            # FK → dim_customer
        "product_id",             # FK → dim_product
        "date_key",               # FK → dim_date
        F.col("region_id"),       # FK → dim_region
        F.col("payment_id"),      # FK → dim_payment
        # Measures
        F.col("quantity").cast(IntegerType()),
        F.col("price").cast("double"),
        F.col("discount").cast("double"),
        F.col("total_amount").cast("double"),
        F.col("shipping_cost").cast("double"),
        F.col("profit_margin").cast("double"),
        F.col("delivery_time_days").cast(IntegerType()),
        # بيانات إضافية مفيدة
        F.when(F.col("returned") == "Yes", 1).otherwise(0).cast(IntegerType())
         .alias("is_returned"),
    )

    return fact


def write_gold(df, name: str, mode: str = "overwrite"):
    """يكتب DataFrame كـ Parquet في Gold layer."""
    path = f"{HDFS_GOLD}{name}"
    log.info("💾 Writing %-25s → %s  (mode=%s, rows=%d)",
             name, path, mode, df.count())
    df.write.mode(mode).format("parquet").save(path)
    log.info("✅ %s written successfully.", name)


# ─── Main ────────────────────────────────────────────────────────────────────
def transform_to_star_schema(**context):
    spark = get_spark()

    log.info("📖 Reading Bronze layer from: %s", HDFS_BRONZE)
    df = spark.read.parquet(HDFS_BRONZE)
    log.info("📊 Total Bronze records: %d", df.count())

    # ── Dimensions ──────────────────────────────────────────────────────────
    dim_customer = build_dim_customer(df)
    dim_product  = build_dim_product(df)
    dim_date     = build_dim_date(df)

    # dim_region و dim_payment محتاجينهم في الـ fact join
    dim_region  = build_dim_region(df)
    dim_payment = build_dim_payment(df)

    # ── Fact ────────────────────────────────────────────────────────────────
    # نعمل alias للـ region column عشان الـ join يشتغل صح
    df_for_fact = df.withColumnRenamed("region", "region_name")
    fact_sales  = build_fact_sales(df_for_fact, dim_region, dim_payment)

    # ── Write Gold ──────────────────────────────────────────────────────────
    write_gold(dim_customer, "dim_customer")
    write_gold(dim_product,  "dim_product")
    write_gold(dim_date,     "dim_date")
    write_gold(dim_region,   "dim_region")
    write_gold(dim_payment,  "dim_payment")
    write_gold(fact_sales,   "fact_sales", mode="append")

    log.info("🎉 Star Schema built successfully in Gold layer!")
    spark.stop()


if __name__ == "__main__":
    transform_to_star_schema()
