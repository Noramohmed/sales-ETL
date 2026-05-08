-- ============================================================
--  Snowflake DDL — Sales Data Warehouse (Star Schema)
--  شغّله في Snowflake Worksheet قبل ما تشغّل الـ pipeline
-- ============================================================

-- 1. إنشاء Database و Schema و Warehouse
-- ─────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS SALES_DWH;
USE DATABASE SALES_DWH;

CREATE SCHEMA IF NOT EXISTS GOLD_LAYER;
USE SCHEMA GOLD_LAYER;

CREATE WAREHOUSE IF NOT EXISTS SALES_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME  = TRUE;

USE WAREHOUSE SALES_WH;


-- ============================================================
--  DIMENSION TABLES
-- ============================================================

-- 2. dim_customer
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id     VARCHAR(20)  NOT NULL PRIMARY KEY,
    customer_age    INTEGER,
    customer_gender VARCHAR(10)
);

COMMENT ON TABLE dim_customer IS 'بيانات العملاء';


-- 3. dim_product
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_product (
    product_id  VARCHAR(20) NOT NULL PRIMARY KEY,
    category    VARCHAR(50)
);

COMMENT ON TABLE dim_product IS 'بيانات المنتجات والفئات';


-- 4. dim_date  (Calendar Table)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INTEGER     NOT NULL PRIMARY KEY,  -- YYYYMMDD
    full_date     DATE        NOT NULL,
    year          INTEGER     NOT NULL,
    quarter       INTEGER     NOT NULL,              -- 1-4
    month         INTEGER     NOT NULL,              -- 1-12
    month_name    VARCHAR(15) NOT NULL,
    day           INTEGER     NOT NULL,              -- 1-31
    day_name      VARCHAR(10) NOT NULL,
    week_of_year  INTEGER     NOT NULL,
    is_weekend    BOOLEAN     NOT NULL
);

COMMENT ON TABLE dim_date IS 'جدول التقويم — مشتق من order_date';


-- 5. dim_region
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_region (
    region_id    INTEGER     NOT NULL PRIMARY KEY,
    region_name  VARCHAR(50) NOT NULL
);

COMMENT ON TABLE dim_region IS 'المناطق الجغرافية';


-- 6. dim_payment
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_payment (
    payment_id      INTEGER     NOT NULL PRIMARY KEY,
    payment_method  VARCHAR(50) NOT NULL
);

COMMENT ON TABLE dim_payment IS 'طرق الدفع';


-- ============================================================
--  FACT TABLE
-- ============================================================

-- 7. fact_sales
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_sales (
    -- Surrogate / natural key
    order_id          VARCHAR(20)    NOT NULL,

    -- Foreign Keys → Dimensions
    customer_id       VARCHAR(20)    NOT NULL  REFERENCES dim_customer(customer_id),
    product_id        VARCHAR(20)    NOT NULL  REFERENCES dim_product(product_id),
    date_key          INTEGER        NOT NULL  REFERENCES dim_date(date_key),
    region_id         INTEGER                  REFERENCES dim_region(region_id),
    payment_id        INTEGER                  REFERENCES dim_payment(payment_id),

    -- Measures (Metrics)
    quantity          INTEGER,
    price             FLOAT,
    discount          FLOAT,            -- نسبة الخصم (0.0 → 1.0)
    total_amount      FLOAT,
    shipping_cost     FLOAT,
    profit_margin     FLOAT,
    delivery_time_days INTEGER,
    is_returned       INTEGER           -- 0 = No, 1 = Yes
);

COMMENT ON TABLE fact_sales IS 'جدول المعاملات الرئيسي — كل row = أوردر واحد';


-- ============================================================
--  VALIDATION QUERIES  — شغّلها بعد الـ Load للتحقق
-- ============================================================

-- عدد الـ records في كل جدول
SELECT 'dim_customer' AS tbl, COUNT(*) AS cnt FROM dim_customer
UNION ALL
SELECT 'dim_product',                          COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_date',                             COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_region',                           COUNT(*) FROM dim_region
UNION ALL
SELECT 'dim_payment',                          COUNT(*) FROM dim_payment
UNION ALL
SELECT 'fact_sales',                           COUNT(*) FROM fact_sales;


-- أجمالي المبيعات per region
SELECT
    r.region_name,
    COUNT(f.order_id)       AS total_orders,
    SUM(f.total_amount)     AS total_revenue,
    AVG(f.profit_margin)    AS avg_profit_margin
FROM fact_sales f
JOIN dim_region r ON f.region_id = r.region_id
GROUP BY r.region_name
ORDER BY total_revenue DESC;


-- مبيعات per category per quarter
SELECT
    p.category,
    d.year,
    d.quarter,
    COUNT(f.order_id)    AS orders,
    SUM(f.total_amount)  AS revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_date    d ON f.date_key   = d.date_key
GROUP BY p.category, d.year, d.quarter
ORDER BY d.year, d.quarter, revenue DESC;


-- نسبة المرتجعات per payment method
SELECT
    pm.payment_method,
    COUNT(*)                                    AS total_orders,
    SUM(f.is_returned)                          AS returned_orders,
    ROUND(SUM(f.is_returned) * 100.0 / COUNT(*), 2) AS return_rate_pct
FROM fact_sales f
JOIN dim_payment pm ON f.payment_id = pm.payment_id
GROUP BY pm.payment_method
ORDER BY return_rate_pct DESC;
