"""
02_extract_to_hdfs.py  (E — Extract)
-------------------------------------
يقرأ الـ messages من Kafka topic "sales_events"
ويكتبها في HDFS Bronze Layer كـ Parquet files.

Bronze Layer = بيانات خام كما هي بدون أي تعديل.
"""

import os
import json
import logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

os.environ["HADOOP_USER_NAME"] = "root"

# ─── Config ─────────────────────────────────────────────────────────────────
KAFKA_BROKER      = "kafka:29092"
KAFKA_TOPIC       = "sales_events"
KAFKA_GROUP       = "sales_etl_group"
HDFS_BRONZE_PATH  = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/sales/"
CONSUME_TIMEOUT   = 15_000          # ms — ينتظر 15 ثانية لو مفيش messages جديدة
MAX_RECORDS       = 5_000           # أقصى عدد records كل run


# ─── Sales schema ────────────────────────────────────────────────────────────
SALES_SCHEMA = StructType([
    StructField("order_id",          StringType(),  True),
    StructField("customer_id",       StringType(),  True),
    StructField("product_id",        StringType(),  True),
    StructField("category",          StringType(),  True),
    StructField("price",             DoubleType(),  True),
    StructField("discount",          DoubleType(),  True),
    StructField("quantity",          IntegerType(), True),
    StructField("payment_method",    StringType(),  True),
    StructField("order_date",        StringType(),  True),
    StructField("delivery_time_days",IntegerType(), True),
    StructField("region",            StringType(),  True),
    StructField("returned",          StringType(),  True),
    StructField("total_amount",      DoubleType(),  True),
    StructField("shipping_cost",     DoubleType(),  True),
    StructField("profit_margin",     DoubleType(),  True),
    StructField("customer_age",      IntegerType(), True),
    StructField("customer_gender",   StringType(),  True),
])


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SalesETL_Extract")
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


def consume_from_kafka() -> list:
    """يستهلك messages من Kafka ويرجعها كـ list of dicts."""
    for attempt in range(5):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=KAFKA_GROUP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=CONSUME_TIMEOUT,
            )
            log.info("✅ Connected to Kafka. Consuming messages ...")
            break
        except NoBrokersAvailable:
            log.warning("⏳ Kafka not ready, retry %d/5 ...", attempt + 1)
            time.sleep(5)
    else:
        raise RuntimeError("❌ Cannot connect to Kafka")

    records = []
    for message in consumer:
        records.append(message.value)
        if len(records) >= MAX_RECORDS:
            log.info("📦 Reached MAX_RECORDS limit (%d). Stopping consumption.", MAX_RECORDS)
            break

    consumer.close()
    log.info("📥 Consumed %d records from Kafka topic '%s'", len(records), KAFKA_TOPIC)
    return records


def extract_to_hdfs(**context):
    """
    Airflow callable:
    1. يقرأ من Kafka
    2. يحول الـ records لـ Spark DataFrame
    3. يكتب Parquet في HDFS Bronze Layer
    """
    records = consume_from_kafka()

    if not records:
        log.warning("⚠️  No records consumed. Skipping HDFS write.")
        return

    spark = get_spark()
    log.info("⚙️  Creating Spark DataFrame from %d records ...", len(records))

    df = spark.createDataFrame(records, schema=SALES_SCHEMA)

    # إحصائيات سريعة
    log.info("📊 Records to write: %d", df.count())
    log.info("📂 Writing to HDFS Bronze: %s", HDFS_BRONZE_PATH)

    df.write \
        .mode("append") \
        .format("parquet") \
        .save(HDFS_BRONZE_PATH)

    log.info("✅ Extract complete! Data saved to Bronze layer.")
    spark.stop()


# ─── تشغيل مباشر ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    extract_to_hdfs()
