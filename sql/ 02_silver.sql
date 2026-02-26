USE WAREHOUSE COMPUTE_WH;

USE DATABASE SNOWFLAKE_LEARNING_DB;

CREATE SCHEMA IF NOT EXISTS SILVER;

CREATE OR REPLACE TABLE SILVER.STORE_SALES AS
SELECT
    ss.ss_sold_date_sk,
    ss.ss_store_sk,
    ss.ss_customer_sk,
    ss.ss_quantity,
    ss.ss_sales_price,
FROM BRONZE.STORE_SALES ss
JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM dd
    ON ss.ss_sold_date_sk = dd.d_date_sk
WHERE dd.d_year BETWEEN 1998 AND 1999
AND ss.ss_sales_price IS NOT NULL
AND ss.ss_quantity > 0;

DESC TABLE SILVER.STORE_SALES;

SELECT MIN(ss.ss_sales_price), MAX(ss.ss_sales_price)
FROM SILVER.STORE_SALES ss;

SELECT COUNT(*) FROM SILVER.STORE_SALES WHERE ss_sales_price IS NULL;

SELECT COUNT(*) FROM SILVER.STORE_SALES WHERE ss_quantity <= 0;