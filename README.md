# 🛒 Sales ETL Pipeline — دليل التشغيل الكامل

## نظرة عامة على المشروع

Pipeline بيأخذ بيانات Sales من CSV ويبعتها كـ real-time events عبر **Kafka**،
ثم يخزنها في **HDFS** (Bronze Layer)، يحولها لـ **Star Schema** بـ **Spark** (Gold Layer)،
وأخيراً يرفعها لـ **Snowflake** Data Warehouse — وكل ده متحكم فيه بـ **Airflow DAG**.

```
CSV → [Kafka Producer] → Kafka Topic → [Spark Extract] → HDFS Bronze
                                                               ↓
                                              [Spark Transform] → HDFS Gold (Star Schema)
                                                               ↓
                                              [Spark Load] → Snowflake DWH
                                                    ↑
                                              Airflow DAG (يتحكم في كل حاجة)
```

---

## 📁 هيكل الفولدر

```
sales_etl_project/
├── docker-compose.yaml              ← كل الـ services
├── .env                             ← متغيرات البيئة (Snowflake credentials هنا)
├── download_jars.sh                 ← script لتحميل Snowflake JARs
├── snowflake_ddl.sql                ← SQL لإنشاء الـ DWH في Snowflake
│
├── data/
│   └── ecommerce_sales_34500.csv   ← بيانات الـ Sales
│
├── jars/                            ← Snowflake JARs (بيتحملوا بـ download_jars.sh)
│
├── dags/
│   ├── sales_etl_dag.py            ← Airflow DAG الرئيسي
│   └── scripts/
│       ├── 01_kafka_producer.py    ← يبعت الـ CSV على Kafka
│       ├── 02_extract_to_hdfs.py  ← E: يقرأ من Kafka → HDFS Bronze
│       ├── 03_transform_spark.py  ← T: Spark → Star Schema في Gold
│       └── 04_load_snowflake.py   ← L: HDFS Gold → Snowflake
│
├── logs/                            ← Airflow logs
├── plugins/                         ← Airflow plugins (فاضي)
├── config/                          ← Airflow config (فاضي)
└── notebooks/                       ← Jupyter notebooks (للاستكشاف)
```

---

## 🚀 خطوات التشغيل (خطوة بخطوة)

### الخطوة 1 — المتطلبات

تأكد إن عندك:
- **Docker Desktop** مثبّت وشغّال
- **8 GB RAM** على الأقل متاحة لـ Docker
- **Python 3.8+** (اختياري — للتشغيل اليدوي)

---

### الخطوة 2 — تحضير الـ Snowflake

**2.1** افتح [snowflake.com](https://app.snowflake.com) وسجّل دخول.

**2.2** افتح **Worksheets** والصق محتوى ملف `snowflake_ddl.sql` وشغّله.
هيعمل:
- Database: `SALES_DWH`
- Schema: `GOLD_LAYER`
- Warehouse: `SALES_WH`
- كل الـ tables (dim_* + fact_sales)

**2.3** عدّل ملف `.env` بيانات Snowflake بتاعتك:
```env
SF_ACCOUNT=abc123.us-east-1          # من Settings > Account
SF_USER=john                          # اليوزرنيم بتاعك
SF_PASSWORD=MySecret123!              # الباسورد بتاعك
```

**2.4** عدّل ملف `dags/scripts/04_load_snowflake.py` في الـ `SNOWFLAKE_OPTIONS`:
```python
SNOWFLAKE_OPTIONS = {
    "sfURL":       "abc123.us-east-1.snowflakecomputing.com",
    "sfUser":      "john",
    "sfPassword":  "MySecret123!",
    ...
}
```

---

### الخطوة 3 — تحميل الـ JARs

```bash
# من داخل فولدر المشروع
cd sales_etl_project
chmod +x download_jars.sh
./download_jars.sh
```

هيحمّل:
- `spark-snowflake_2.12-2.14.0-spark_3.2.jar`
- `snowflake-jdbc-3.15.0.jar`

في مجلد `jars/`.

---

### الخطوة 4 — تشغيل الـ Containers

```bash
# من داخل فولدر المشروع
docker-compose up -d
```

**استنى 2-3 دقايق** حتى كل الـ services تشتغل، ثم تحقق:

```bash
docker ps
```

يجب أن ترى هذه الـ containers كلها في حالة `Up`:

| Container          | الوظيفة              |
|--------------------|----------------------|
| airflow-webserver  | Airflow UI           |
| airflow-scheduler  | Airflow Scheduler    |
| airflow-triggerer  | Airflow Triggerer    |
| postgres_airflow   | Airflow Database     |
| zookeeper          | Kafka Coordinator    |
| kafka              | Message Broker       |
| hadoop-namenode    | HDFS Name Node       |
| hadoop-datanode1   | HDFS Data Node 1     |
| hadoop-datanode2   | HDFS Data Node 2     |
| resourcemanager    | YARN Resource Mgr    |
| hadoop-nodemanager | YARN Node Manager 1  |
| hadoop-nodemanager2| YARN Node Manager 2  |
| spark-jupyter      | Spark + Jupyter      |

---

### الخطوة 5 — إنشاء الـ HDFS Directories

```bash
docker exec hadoop-namenode bash -c "
    hdfs dfs -mkdir -p /user/root/datalake/bronze/sales &&
    hdfs dfs -mkdir -p /user/root/datalake/gold &&
    hdfs dfs -chmod -R 777 /user/root &&
    echo 'Directories created!'
"
```

تحقق من الـ HDFS UI: [http://localhost:9870](http://localhost:9870)

---

### الخطوة 6 — تشغيل الـ Airflow DAG

**6.1** افتح Airflow UI: [http://localhost:18080](http://localhost:18080)
- Username: `airflow`
- Password: `airflow`

**6.2** ابحث عن DAG اسمه `sales_etl_pipeline`.

**6.3** فعّل الـ DAG بالضغط على الـ toggle (الزر الأزرق على اليسار).

**6.4** اضغط زرار ▶️ **Trigger DAG** لتشغيله يدوياً.

**6.5** راقب التقدم في **Graph View** — كل task هيتحول للأخضر لما يخلص.

---

### الخطوة 7 — التحقق من النتائج

**تحقق من Kafka:**
```bash
docker exec kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic sales_events \
    --from-beginning \
    --max-messages 5
```

**تحقق من HDFS Bronze:**
```bash
docker exec hadoop-namenode hdfs dfs -ls /user/root/datalake/bronze/sales/
```

**تحقق من HDFS Gold:**
```bash
docker exec hadoop-namenode hdfs dfs -ls /user/root/datalake/gold/
```

**تحقق من Snowflake:**
```sql
-- في Snowflake Worksheet
USE DATABASE SALES_DWH;
USE SCHEMA GOLD_LAYER;

SELECT 'dim_customer' AS tbl, COUNT(*) AS cnt FROM dim_customer
UNION ALL SELECT 'dim_product',  COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_date',     COUNT(*) FROM dim_date
UNION ALL SELECT 'dim_region',   COUNT(*) FROM dim_region
UNION ALL SELECT 'dim_payment',  COUNT(*) FROM dim_payment
UNION ALL SELECT 'fact_sales',   COUNT(*) FROM fact_sales;
```

---

## 🌐 الـ URLs المهمة

| Service      | URL                         | Username | Password |
|--------------|-----------------------------|----------|----------|
| Airflow UI   | http://localhost:18080      | airflow  | airflow  |
| HDFS UI      | http://localhost:9870       | —        | —        |
| YARN UI      | http://localhost:8088       | —        | —        |
| Jupyter Lab  | http://localhost:8899       | —        | —        |
| Spark UI     | http://localhost:4040       | —        | —        |

---

## ⭐ Star Schema Diagram

```
                        dim_date
                       ┌──────────────┐
                       │ date_key (PK)│
                       │ full_date    │
                       │ year         │
                       │ quarter      │
                       │ month        │
                       │ day          │
                       │ is_weekend   │
                       └──────┬───────┘
                              │
dim_customer          fact_sales          dim_product
┌─────────────┐      ┌──────────────┐    ┌─────────────┐
│customer_id  │◄─────│customer_id   │───►│product_id   │
│customer_age │      │product_id    │    │category     │
│customer_gender│    │date_key      │    └─────────────┘
└─────────────┘      │region_id     │
                     │payment_id    │    dim_region
dim_payment          │order_id      │    ┌─────────────┐
┌─────────────┐      │quantity      │───►│region_id    │
│payment_id   │◄─────│price         │    │region_name  │
│payment_method│     │discount      │    └─────────────┘
└─────────────┘      │total_amount  │
                     │shipping_cost │
                     │profit_margin │
                     │is_returned   │
                     └──────────────┘
```

---

## ❗ حل المشاكل الشائعة

### Kafka لا يرد
```bash
# تحقق من إن zookeeper شغّال الأول
docker logs zookeeper | tail -20
docker logs kafka | tail -20
```

### HDFS لا يكتب
```bash
# تحقق من الـ namenode
docker logs hadoop-namenode | tail -30
# تحقق من الـ datanodes
docker exec hadoop-namenode hdfs dfsadmin -report
```

### Spark يفشل في YARN
```bash
# افتح YARN UI على localhost:8088 وشوف الـ application logs
# أو
docker logs resourcemanager | tail -30
```

### Snowflake Connection فشل
- تأكد من الـ `sfURL` صح (بدون `https://` في الأول)
- تأكد إن الـ JARs في مجلد `jars/`
- تأكد إن الـ warehouse شغّال في Snowflake

---

## 🛑 إيقاف كل حاجة

```bash
docker-compose down

# لو عايز تمسح الـ volumes (HDFS data) كمان
docker-compose down -v
```
