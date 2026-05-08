"""
04_load_snowflake.py  (L — Load)
----------------------------------
يقرأ الـ Star Schema من HDFS Gold Layer
ويرفعها لـ Snowflake Data Warehouse.

Strategy:
    Dimensions → overwrite (swap via temp table)  — بيانات reference تتحدث كلها
    fact_sales  → append                          — بيانات حقيقية تتراكم

⚠️  قبل التشغيل:
    1. عدّل الـ Config section بالبيانات بتاعتك
    2. حمّل Snowflake Spark Connector JAR (شوف README)
"""

import os
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

os.environ["HADOOP_USER_NAME"] = "root"

# ─── ⚠️  CONFIG — عدّل هنا ─────────────────────────────────────────────────
SNOWFLAKE_OPTIONS = {
    "sfURL":       "LG93326.us-east-1.snowflakecomputing.com",   # مثال: abc123.us-east-1
    "sfUser":      "NoraMohamed",
    "sfPassword":  "NZA78ecfA7M49xM",
    "sfDatabase":  "SALES_DWH",
    "sfSchema":    "GOLD_LAYER",
    "sfWarehouse": "SALES_WH",
    "sfRole":      "SYSADMIN",                              # أو أي role عندك
}

HDFS_GOLD = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"

# الجداول اللي هيتعملوا overwrite (dimensions)
DIMENSION_TABLES = [
    "dim_customer",
    "dim_product",
    "dim_date",
    "dim_region",
    "dim_payment",
]

# الجداول اللي هيتعملها append (facts)
FACT_TABLES = [
    "fact_sales",
]
# ────────────────────────────────────────────────────────────────────────────


def get_spark() -> SparkSession:
    """
    ملاحظة: محتاج تضيف Snowflake JARs في --jars لما تشغل spark-submit
    مثال:
        spark-submit \\
            --jars /opt/spark/jars/snowflake-jdbc-3.15.0.jar,/opt/spark/jars/spark-snowflake_2.12-2.14.0-spark_3.2.jar \\
            04_load_snowflake.py
    """
    return (
        SparkSession.builder
        .appName("SalesETL_Load")
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


def table_exists_in_snowflake(spark: SparkSession, table_name: str) -> bool:
    """يتحقق إن الجدول موجود في Snowflake."""
    query = f"""
        SELECT TABLE_NAME
        FROM {SNOWFLAKE_OPTIONS['sfDatabase']}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{SNOWFLAKE_OPTIONS['sfSchema']}'
          AND TABLE_NAME = '{table_name.upper()}'
    """
    try:
        result = (
            spark.read
            .format("net.snowflake.spark.snowflake")
            .options(**SNOWFLAKE_OPTIONS)
            .option("query", query)
            .load()
        )
        return result.count() > 0
    except Exception:
        return False


def run_snowflake_query(spark: SparkSession, sql: str):
    """ينفذ SQL مباشرة في Snowflake."""
    utils = spark._jvm.net.snowflake.spark.snowflake.Utils
    utils.runQuery(SNOWFLAKE_OPTIONS, sql)


def load_dimension(spark: SparkSession, table_name: str):
    """
    يحمّل dimension table بـ atomic swap:
        1. اكتب في temp table
        2. SWAP الـ temp مع الـ final
        3. احذف الـ temp
    """
    final_table = f"{SNOWFLAKE_OPTIONS['sfDatabase']}.{SNOWFLAKE_OPTIONS['sfSchema']}.{table_name.upper()}"
    temp_table  = f"{final_table}_TEMP"

    log.info("📤 Loading dimension: %s ...", table_name)
    df = spark.read.parquet(f"{HDFS_GOLD}{table_name}")
    log.info("   Rows to load: %d", df.count())

    # كتابة في temp
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**SNOWFLAKE_OPTIONS) \
        .option("dbtable", temp_table) \
        .mode("overwrite") \
        .save()

    # Atomic swap
    if table_exists_in_snowflake(spark, table_name):
        run_snowflake_query(spark, f"ALTER TABLE {final_table} SWAP WITH {temp_table}")
        run_snowflake_query(spark, f"DROP TABLE IF EXISTS {temp_table}")
        log.info("   🔄 Swapped temp → final for %s", table_name)
    else:
        run_snowflake_query(spark, f"ALTER TABLE {temp_table} RENAME TO {final_table}")
        log.info("   🆕 Created new table %s", table_name)

    log.info("   ✅ %s loaded successfully.", table_name)


def load_fact(spark: SparkSession, table_name: str):
    """
    يحمّل fact table بـ append mode (بيانات تاريخية تتراكم).
    """
    target = f"{SNOWFLAKE_OPTIONS['sfDatabase']}.{SNOWFLAKE_OPTIONS['sfSchema']}.{table_name.upper()}"
    log.info("📤 Appending fact table: %s ...", table_name)
    df = spark.read.parquet(f"{HDFS_GOLD}{table_name}")
    log.info("   Rows to append: %d", df.count())

    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**SNOWFLAKE_OPTIONS) \
        .option("dbtable", target) \
        .mode("append") \
        .save()

    log.info("   ✅ %s appended successfully.", table_name)


def load_to_snowflake(**context):
    """Airflow callable."""
    spark = get_spark()

    log.info("❄️  Starting Snowflake load ...")

    for table in DIMENSION_TABLES:
        load_dimension(spark, table)

    for table in FACT_TABLES:
        load_fact(spark, table)

    log.info("🎉 All tables loaded to Snowflake successfully!")
    spark.stop()


if __name__ == "__main__":
    load_to_snowflake()
