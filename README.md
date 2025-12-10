# Databricks Lakehouse: End-to-End Production Grade Medallion Architecture - OLAP

![Status](https://img.shields.io/badge/Status-Completed-success)
![Tech Stack](https://img.shields.io/badge/Stack-Pyspark%20|%20Databricks%20|%20Spark%20SQL%20|%20Python-blue)

# 📖 Project Overview
This project implements a production-grade **Lakehouse Architecture** using **Apache Spark** and **Databricks**. It simulates a real-world data pipeline that ingests raw sales data, processes it through a **Medallion Architecture** (Bronze, Silver, Gold), and models it into a **Star Schema** for OLAP reporting.

The pipeline is designed to be **idempotent**, **fault-tolerant**, and handles **incremental loading** with **Slowly Changing Dimensions (SCD Type 1)**.

---

# Architecture Design:  **Medallion Architecture**


### 1. 🥉 Bronze Layer (Raw Ingestion)
* **Role:** Raw data is ingested from source and is stored temporarily.
* **Strategy:** **Transient / Stateless**.
* **Note:** There is a persistent state as well where raw ingested data is stored permanently.
* **Incremental Logic:** Uses a **Watermark** strategy anchored to the *Silver* layer (Persistent History) to ensure no data loss even if the Bronze table is wiped.
* **Key Feature:** Prevents "empty batch" failures by decoupling the watermark source from the transient buffer.

### 2. 🥈 Silver Layer (Cleansed & Enriched)
* **Role:** The raw data dumped in bronze layer is deduplicated, cleaned and merged in this layer and is stored permanently.
* **Strategy:** **Upsert (Merge)**.
* **Transformation:** Standardizes formats (Date casting, Text normalization) and removes duplicates.
* **Logic:** Uses `MERGE INTO` to update existing records and insert new ones, ensuring the history remains accurate over time.

### 3. 🥇 Gold Layer (Dimensional Modeling / OLAP)
* **Role:** Optimized for BI & Analytics (Star Schema).
* **Strategy:** **SCD Type 1 (Slowly Changing Dimensions)**.
* **Key Features:**
    * **Surrogate Keys:** Uses `GENERATED ALWAYS AS IDENTITY` to maintain stable, permanent IDs (unlike `row_number()` which shifts daily).
    * **Idempotency:** The Fact table load uses `MERGE` instead of `INSERT` to prevent duplicate sales if the pipeline is re-run.

---

# 🧩 Data Model (Star Schema)

The Gold layer is modeled as a classic Star Schema for optimal performance in tools like Power BI or Tableau.

* **Fact Table:**
    * `FactSales` (Order metrics, Foreign Keys to Dimensions)
* **Dimension Tables:**
    * `DimCustomers` (Customer details, handles profile updates)
    * `DimProducts` (Product catalog)
    * `DimRegions` (Geographic hierarchy)
    * `DimPayments` (Payment methods)

---

# 🚀 Key Technical Challenges Solved

### 1. The "Reset Loop" Bug
**Problem:** In typical pipelines, using the Bronze table's max date as a watermark fails if the Bronze table is transient (wiped daily).

**Solution:** I implemented a **Cross-Layer Watermarking** strategy. The Bronze ingestion script queries the **Silver** layer (Target) to find the true last processed date, ensuring robust incremental loads.

### 2. Surrogate Key Instability
**Problem:** Regenerating keys using `row_number()` daily causes keys to shift (e.g., Customer A is ID 1 today, ID 2 tomorrow), corrupting historical facts.

**Solution:** Leveraged **Identity Columns** (`GENERATED ALWAYS AS IDENTITY`) in the Gold Delta tables to ensure keys are immutable and persistent.

### 3. Duplicate Handling
**Problem:** Simple `INSERT INTO` statements in Fact tables can cause duplicates if a job is retried.

**Solution:** Implemented **Idempotent Merges** for the Fact table. The pipeline checks if an `order_id` exists before inserting, making the pipeline safe to run multiple times.

---

# 💻 Code Snippets
## Handling SCD - Type 1. (Slowly Changing Dimensions.)

### Bronze Layer (Transient) - Ingesting Data from Source & Last Updated Watermark from Silver Layer
```python
if spark.catalog.tableExists('datamodeling.silver.silver_table'):
  last_load_date = spark.sql("select max(last_updated) from datamodeling.silver.silver_table").collect()[0][0]
  if last_load_date is None:
    last_load_date = "1900-01-01"
else:
  last_load_date = "1900-01-01"
```

### Silver Layer (Upserting Records from Bronze Layer)
```python
merge into datamodeling.silver.silver_table
using silver_source
on datamodeling.silver.silver_table.order_id = silver_source.order_id
when matched then update set *
when not matched then insert *

``` 
### Gold Layer (Populating Fact - Dimension Tables Incrementally)

```python

%sql
MERGE INTO datamodeling.gold.DimCustomers AS target
USING (
    SELECT DISTINCT customer_id, customer_email, customer_name, Customer_Name_Upper 
    FROM datamodeling.silver.silver_table
) AS source
ON target.customer_id = source.customer_id


WHEN MATCHED AND (
       target.customer_email <> source.customer_email OR
       target.customer_name  <> source.customer_name
     ) THEN
    UPDATE SET 
       target.customer_email = source.customer_email,
       target.customer_name  = source.customer_name,
       target.Customer_Name_Upper = source.customer_name_upper


WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_email, customer_name, Customer_Name_Upper)
    VALUES (source.customer_id, source.customer_email, source.customer_name, source.customer_name_upper);

```




## Updating Fact Table Incrementally

```python
%python

if spark.catalog.tableExists('datamodeling.gold.FactSales'):
    max_order_id = spark.sql("select max(order_id) from datamodeling.gold.FactSales").collect()[0][0]
    if max_order_id is None: max_order_id = 0
else:
    max_order_id = 0

spark.sql(f"""
    SELECT * FROM datamodeling.silver.silver_table 
    WHERE order_id > {max_order_id}
""").createOrReplaceTempView("new_sales_data")

print(f"Processing sales with Order ID greater than: {max_order_id}") 

%sql
MERGE INTO datamodeling.gold.FactSales AS target
USING (
    SELECT 
        C.DimCustomerKey,
        P.DimProductKey,
        R.DimRegionKey,
        PY.DimPaymentKey,
        S.quantity,
        S.unit_price,
        S.order_id,
        current_timestamp() as loaded_date
    FROM 
        new_sales_data S
    -- Join to Dimensions to get the STABLE Surrogate Keys
    LEFT JOIN datamodeling.gold.DimCustomers C ON S.customer_id = C.customer_id
    LEFT JOIN datamodeling.gold.DimProducts P  ON S.product_id = P.product_id
    LEFT JOIN datamodeling.gold.DimRegions R   ON S.country = R.country
    LEFT JOIN datamodeling.gold.DimPayments PY ON S.payment_type = PY.payment_type
) AS source
ON target.order_id = source.order_id


WHEN NOT MATCHED THEN
    INSERT (DimCustomerKey, DimProductKey, DimRegionKey, DimPaymentKey, quantity, unit_price, order_id, loaded_date)
    VALUES (source.DimCustomerKey, source.DimProductKey, source.DimRegionKey, source.DimPaymentKey, source.quantity, source.unit_price, source.order_id, source.loaded_date);





