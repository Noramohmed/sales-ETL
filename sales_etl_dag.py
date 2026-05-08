"""
sales_etl_dag.py
-----------------
Airflow DAG الرئيسي اللي بيتحكم في كل الـ pipeline.

Task Order:
    create_hdfs_dirs
        ↓
    produce_to_kafka          (Kafka Producer)
        ↓
    extract_to_hdfs           (E — Bronze Layer)
        ↓
    transform_star_schema     (T — Gold Layer)
        ↓
    load_to_snowflake         (L — Snowflake DWH)
        ↓
    validate_output           (Data Quality Check)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os

# ─── Default Args ────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "data_engineer",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
}

# ─── DAG Definition ──────────────────────────────────────────────────────────
with DAG(
    dag_id="sales_etl_pipeline",
    description="Sales Data ETL: Kafka → HDFS Bronze → Spark Gold → Snowflake",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",          # بيشتغل كل يوم
    catchup=False,
    max_active_runs=1,
    tags=["sales", "etl", "kafka", "hdfs", "spark", "snowflake"],
) as dag:

    # ── Task 0: Create HDFS directories ─────────────────────────────────────
    create_hdfs_dirs = BashOperator(
        task_id="create_hdfs_dirs",
        bash_command="""
            docker exec hadoop-namenode bash -c "
                hdfs dfs -mkdir -p /user/root/datalake/bronze/sales &&
                hdfs dfs -mkdir -p /user/root/datalake/gold &&
                hdfs dfs -chmod -R 777 /user/root &&
                echo 'HDFS directories ready ✅'
            "
        """,
        doc_md="""
        ### Create HDFS Directories
        ينشئ مجلدات Bronze و Gold في HDFS لو مش موجودين.
        """,
    )

    # ── Task 1: Kafka Producer ───────────────────────────────────────────────
    def _produce(**context):
        from scripts.kafka_producer import produce_sales_events
        produce_sales_events(**context)

    produce_to_kafka = PythonOperator(
        task_id="produce_to_kafka",
        python_callable=_produce,
        doc_md="""
        ### Kafka Producer
        يقرأ الـ CSV row by row ويبعت كل record كـ JSON message
        على Kafka topic `sales_events`.
        يستخدم XCom لتتبع الـ offset بين الـ runs.
        """,
    )

    # ── Task 2: Extract → HDFS Bronze ────────────────────────────────────────
    # يشتغل في spark-jupyter container عشان محتاج PySpark + Kafka
    extract_bronze = BashOperator(
        task_id="extract_to_hdfs",
        bash_command="""
            docker exec spark-jupyter bash -c "
                spark-submit \\
                    --master yarn \\
                    --deploy-mode client \\
                    --executor-memory 512m \\
                    --driver-memory 512m \\
                    /opt/airflow/dags/scripts/02_extract_to_hdfs.py
            "
        """,
        execution_timeout=timedelta(minutes=30),
        doc_md="""
        ### Extract to HDFS Bronze
        يستهلك الـ messages من Kafka
        ويكتبها كـ Parquet files في HDFS Bronze Layer.
        """,
    )

    # ── Task 3: Transform → Gold Star Schema ─────────────────────────────────
    transform_gold = BashOperator(
        task_id="transform_star_schema",
        bash_command="""
            docker exec spark-jupyter bash -c "
                spark-submit \\
                    --master yarn \\
                    --deploy-mode client \\
                    --executor-memory 512m \\
                    --driver-memory 512m \\
                    /opt/airflow/dags/scripts/03_transform_spark.py
            "
        """,
        execution_timeout=timedelta(minutes=45),
        doc_md="""
        ### Transform — Star Schema
        يشغّل Spark job يبني Star Schema في Gold Layer:
        - fact_sales
        - dim_customer, dim_product, dim_date, dim_region, dim_payment
        """,
    )

    # ── Task 4: Load → Snowflake ─────────────────────────────────────────────
    load_snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command="""
            docker exec spark-jupyter bash -c "
                spark-submit \\
                    --master yarn \\
                    --deploy-mode client \\
                    --jars /opt/spark/jars/snowflake-jdbc-3.15.0.jar,/opt/spark/jars/spark-snowflake_2.12-2.14.0-spark_3.2.jar \\
                    --executor-memory 512m \\
                    /opt/airflow/dags/scripts/04_load_snowflake.py
            "
        """,
        execution_timeout=timedelta(minutes=60),
        doc_md="""
        ### Load to Snowflake
        يرفع Gold Layer tables لـ Snowflake DWH.
        Dimensions → overwrite (atomic swap)
        fact_sales  → append
        """,
    )

    # ── Task 5: Data Quality Validation ──────────────────────────────────────
    def _validate(**context):
        """
        Validation بسيطة بعد الـ load:
        - تتحقق إن عدد الـ records في Snowflake منطقي
        - تتحقق إن مفيش null values في الـ keys الأساسية
        """
        import logging
        log = logging.getLogger(__name__)

        # هنا ممكن تضيف connection لـ Snowflake وتعمل queries حقيقية
        # مثال بـ snowflake-connector-python:
        #
        # import snowflake.connector
        # conn = snowflake.connector.connect(
        #     user=os.environ["SF_USER"],
        #     password=os.environ["SF_PASSWORD"],
        #     account=os.environ["SF_ACCOUNT"],
        #     database="SALES_DWH",
        #     schema="GOLD_LAYER",
        # )
        # cur = conn.cursor()
        # cur.execute("SELECT COUNT(*) FROM fact_sales")
        # count = cur.fetchone()[0]
        # assert count > 0, "fact_sales is empty!"
        # log.info("✅ Validation passed: fact_sales has %d rows", count)

        log.info("✅ Validation task placeholder — add your checks here.")

    validate = PythonOperator(
        task_id="validate_output",
        python_callable=_validate,
        doc_md="""
        ### Data Quality Validation
        يتحقق من جودة البيانات بعد الـ load.
        """,
    )

    # ── Task Dependencies (الترتيب) ──────────────────────────────────────────
    create_hdfs_dirs >> produce_to_kafka >> extract_bronze >> transform_gold >> load_snowflake >> validate
