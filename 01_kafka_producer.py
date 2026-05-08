"""
01_kafka_producer.py
--------------------
يقرأ الـ CSV row by row ويبعت كل row كـ JSON message على Kafka topic
كأنها بيانات real-time بتيجي من نظام POS أو متجر إلكتروني.

شغّله من Airflow كـ PythonOperator أو يدوياً:
    python 01_kafka_producer.py
"""

import json
import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────
KAFKA_BROKER   = "kafka:29092"          # من داخل Docker network
KAFKA_TOPIC    = "sales_events"
CSV_PATH       = "/opt/airflow/data/ecommerce_sales_34500.csv"
DELAY_SECONDS  = 0.05                  # 0.05 ثانية بين كل record = ~20 record/sec
BATCH_SIZE     = 500                   # عدد الـ records اللي هنبعتها كل run


def get_producer(retries: int = 5) -> KafkaProducer:
    """يحاول يتصل بـ Kafka ويعيد KafkaProducer."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",            # ضمان إن الـ message اتكتبت
                retries=3,
            )
            log.info("✅ Connected to Kafka broker: %s", KAFKA_BROKER)
            return producer
        except NoBrokersAvailable:
            log.warning("⏳ Kafka not ready, retry %d/%d ...", attempt, retries)
            time.sleep(5)
    raise RuntimeError("❌ Could not connect to Kafka after %d attempts" % retries)


def produce_sales_events(**context):
    """
    Airflow callable: يبعت BATCH_SIZE records لـ Kafka.
    يستخدم Airflow XCom لتتبع آخر row اتبعتت (offset).
    """
    # اقرأ الـ offset من الـ run السابق (لو موجود)
    ti = context.get("ti")
    offset = 0
    if ti:
        prev = ti.xcom_pull(task_ids="produce_to_kafka", key="last_offset")
        offset = int(prev) if prev else 0

    df = pd.read_csv(CSV_PATH)
    total_rows = len(df)

    if offset >= total_rows:
        log.info("✅ All %d rows already produced. Nothing to send.", total_rows)
        return

    batch = df.iloc[offset: offset + BATCH_SIZE]
    producer = get_producer()
    sent = 0

    for _, row in batch.iterrows():
        record = row.to_dict()
        # نحول NaN لـ None عشان JSON يقبله
        record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
        producer.send(KAFKA_TOPIC, value=record)
        sent += 1
        time.sleep(DELAY_SECONDS)

    producer.flush()
    producer.close()

    new_offset = offset + sent
    log.info("📤 Sent %d records | Total sent so far: %d / %d", sent, new_offset, total_rows)

    # حفظ الـ offset للـ run الجاي
    if ti:
        ti.xcom_push(key="last_offset", value=new_offset)


# ─── تشغيل مباشر (بدون Airflow) ────────────────────────────────────────────
if __name__ == "__main__":
    log.info("🚀 Starting Kafka producer in standalone mode ...")
    df = pd.read_csv(CSV_PATH)
    producer = get_producer()
    sent = 0
    for _, row in df.iterrows():
        record = row.to_dict()
        record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
        producer.send(KAFKA_TOPIC, value=record)
        sent += 1
        if sent % 500 == 0:
            log.info("📤 Sent %d / %d records ...", sent, len(df))
        time.sleep(DELAY_SECONDS)
    producer.flush()
    producer.close()
    log.info("✅ Done! Total records sent: %d", sent)
