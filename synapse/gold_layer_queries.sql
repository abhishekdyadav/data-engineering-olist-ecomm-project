-- https://adyolistdatastorage.blob.core.windows.net/olistdata/silver/
-- ============================================================
-- OLIST E-COMMERCE - SYNAPSE ANALYTICS GOLD LAYER
-- ============================================================

-- ============================================================
-- CREATE SCHEMA
-- ============================================================
CREATE SCHEMA gold;
GO

-- ============================================================
-- CREATE CREDENTIALS
-- ============================================================
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Your_Password';
GO

CREATE DATABASE SCOPED CREDENTIAL adyadmin WITH IDENTITY = 'Managed Identity';
GO

-- Viewing Credentials
select * from sys.database_credentials
GO

-- ============================================================
-- CREATE EXTERNAL FILE FORMAT
-- ============================================================
CREATE EXTERNAL FILE FORMAT extfileformat WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

-- ============================================================
-- CREATE EXTERNAL DATA SOURCE
-- ============================================================
CREATE EXTERNAL DATA SOURCE goldlayer WITH (
    LOCATION = 'https://adyolistdatastorage.dfs.core.windows.net/olistdata/gold/',
    CREDENTIAL = adyadmin
);
GO

-- ============================================================
-- VIEW 1 - CUSTOMER ORDER ANALYSIS
-- ============================================================
DROP VIEW IF EXISTS gold.final;
GO

CREATE VIEW gold.final
AS
SELECT
    order_status,
    order_purchase_timestamp,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    customer_unique_id,
    customer_city,
    customer_state,
    payment_type,
    payment_installments,
    payment_value,
    price,
    freight_value,
    (price + freight_value)             AS total_order_value,
    actual_delivery_time,
    estimated_delivery_time,
    [Delay Time],
    CASE
        WHEN [Delay Time] < 0  THEN 'Early'
        WHEN [Delay Time] = 0  THEN 'On Time'
        ELSE 'Late'
    END                                 AS delivery_status,
    product_category_name_english       AS category
FROM
    OPENROWSET(
        BULK 'https://adyolistdatastorage.dfs.core.windows.net/olistdata/silver/',
        FORMAT = 'PARQUET'
    ) AS result1
WHERE order_status = 'delivered';
GO

SELECT TOP 10 * FROM gold.final;
GO

-- ============================================================
-- VIEW 2 - LATE DELIVERY ANALYSIS
-- ============================================================
DROP VIEW IF EXISTS gold.late_deliveries;
GO

CREATE VIEW gold.late_deliveries
AS
SELECT
    customer_state,
    customer_city,
    seller_state,
    seller_city,
    product_category_name_english       AS category,
    payment_value,
    actual_delivery_time,
    estimated_delivery_time,
    [Delay Time]                        AS days_late,
    CASE
        WHEN [Delay Time] <= 0  THEN 'On Time / Early'
        WHEN [Delay Time] <= 3  THEN 'Slightly Late (1-3 days)'
        WHEN [Delay Time] <= 7  THEN 'Moderately Late (4-7 days)'
        ELSE 'Very Late (7+ days)'
    END                                 AS delay_category
FROM
    OPENROWSET(
        BULK 'https://adyolistdatastorage.dfs.core.windows.net/olistdata/silver/',
        FORMAT = 'PARQUET'
    ) AS result4
WHERE order_status = 'delivered';
GO

SELECT * FROM gold.late_deliveries;
GO

'''
Gold Layer Summary
gold.final: Customer order details with delivery status flags
gold.late_deliveries: Late delivery breakdown by severity, location, category
'''

-- ============================================================
-- CREATE EXTERNAL TABLE
-- ============================================================
CREATE EXTERNAL TABLE gold.finaltable
WITH (
    LOCATION    = 'Serving/finaltable_v1',
    DATA_SOURCE = goldlayer,
    FILE_FORMAT = extfileformat
)
AS
SELECT * FROM gold.final;
GO

SELECT TOP 10 * FROM gold.finaltable;
GO

-- ============================================================
-- BUSINESS INSIGHT QUERIES
-- ============================================================
--- 1. Category-wise Delivery Performance (Early / On-Time / Late)

SELECT
    category,
    COUNT(*) AS total_orders,

    ROUND(100.0 * SUM(CASE WHEN delivery_status = 'Early' THEN 1 ELSE 0 END) / COUNT(*), 2) AS early_pct,
    ROUND(100.0 * SUM(CASE WHEN delivery_status = 'On Time' THEN 1 ELSE 0 END) / COUNT(*), 2) AS ontime_pct,
    ROUND(100.0 * SUM(CASE WHEN delivery_status = 'Late' THEN 1 ELSE 0 END) / COUNT(*), 2) AS late_pct

FROM gold.final
GROUP BY category
HAVING COUNT(*) > 50
ORDER BY late_pct DESC;