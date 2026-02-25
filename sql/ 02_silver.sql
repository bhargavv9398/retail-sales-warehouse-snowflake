USE WAREHOUSE COMPUTE_WH;

USE DATABASE SNOWFLAKE_LEARNING_DB;

CREATE SCHEMA IF NOT EXISTS SILVER;

CREATE OR REPLACE TABLE SILVER.STORE_SALES_CLEAN AS
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
SELECT count(*) FROM SILVER.STORE_SALES_CLEAN where ss_sales_price is null;
SELECT count(*) FROM SILVER.STORE_SALES_CLEAN where ss_quantity <= 0;