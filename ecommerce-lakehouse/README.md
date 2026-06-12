# E-Commerce Lakehouse Project

## 📋 Table of Contents
* [Overview](#overview)
* [Architecture](#architecture)
* [Data Sources](#data-sources)
* [Directory Structure](#directory-structure)
* [Bronze Layer - Data Ingestion](#bronze-layer---data-ingestion)
* [Silver Layer - Data Transformation](#silver-layer---data-transformation)
* [Gold Layer - Business Analytics](#gold-layer---business-analytics)
* [Configuration Management](#configuration-management)
* [Data Flow](#data-flow)
* [How to Run](#how-to-run)
* [Technologies Used](#technologies-used)
* [Data Models](#data-models)

---

## 🎯 Overview

This project implements a **Medallion Architecture** data lakehouse for e-commerce analytics using **Databricks**, **Apache Spark**, and **Delta Lake**. It processes Brazilian e-commerce data from Olist through three distinct layers:

* **Bronze Layer**: Raw data ingestion from CSV files
* **Silver Layer**: Cleaned, validated, and enriched data
* **Gold Layer**: Business-ready aggregated metrics and analytics

The lakehouse design enables:
* ✅ Data quality enforcement
* ✅ Incremental processing
* ✅ Schema evolution
* ✅ Time travel capabilities
* ✅ ACID transactions
* ✅ Scalable analytics

---

## 🏗️ Architecture

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────┐
│                         RAW DATA LAYER                          │
│                     (CSV Files in /raw-data)                    │
│  • customers      • orders         • order_items                │
│  • products       • order_payments • order_reviews              │
│  • sellers        • geolocation    • product_translations       │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                             │
│                    (Raw Ingestion Layer)                        │
│  Framework: bronze-ingestion-framework.ipynb                    │
│  • CSV → Delta Table conversion                                 │
│  • Adds ingestion_time timestamp                                │
│  • Adds source file path                                        │
│  • No transformations applied                                   │
│  Tables: dev.bronze.bronze_*                                    │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER                             │
│              (Cleaned & Validated Data Layer)                   │
│  Pipeline: Python-based transformation framework                │
│  • Data quality validation                                      │
│  • Deduplication (primary key enforcement)                      │
│  • Null removal                                                 │
│  • Data type conversions (timestamps, dates)                    │
│  • Column capitalization (city names)                           │
│  • Derived fields (date/time components)                        │
│  • Config-driven transformations                                │
│  Tables: dev.silver.silver_*                                    │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER                              │
│                  (Business Analytics Layer)                     │
│  Notebooks: Individual metric calculations                      │
│  • total_revenue          • average_order_value                 │
│  • total_orders           • average_delivery_time               │
│  • top_products           • repeat_customers                    │
│  • top_sellers            • delayed_orders                      │
│  • top_cities             • total_revenue_by_month              │
│  Tables: dev.gold.gold_*                                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📂 Data Sources

The project uses the **Olist Brazilian E-Commerce Public Dataset**, which contains:

### Raw CSV Files (`/notebooks/raw-data/`)

| File | Description | Key Fields |
| --- | --- | --- |
| **olist_customers_dataset.csv** | Customer information | customer_id, customer_city, customer_state |
| **olist_orders_dataset.csv** | Order details | order_id, customer_id, order_status, timestamps |
| **olist_order_items_dataset.csv** | Order line items | order_id, product_id, seller_id, price, freight |
| **olist_order_payments_dataset.csv** | Payment transactions | order_id, payment_type, payment_value |
| **olist_order_reviews_dataset.csv** | Customer reviews | order_id, review_score, review_comment |
| **olist_products_dataset.csv** | Product catalog | product_id, product_category, dimensions |
| **olist_sellers_dataset.csv** | Seller information | seller_id, seller_city, seller_state |
| **olist_geolocation_dataset.csv** | Geographic data | zip_code, lat, lng, city, state |
| **product_category_name_translation.csv** | Category translations | category_name (PT), category_name_english |

---

## 📁 Directory Structure

```
ecommerce-lakehouse/
│
├── notebooks/
│   │
│   ├── raw-data/                          # Source CSV files
│   │   ├── olist_customers_dataset.csv
│   │   ├── olist_orders_dataset.csv
│   │   ├── olist_order_items_dataset.csv
│   │   ├── olist_order_payments_dataset.csv
│   │   ├── olist_order_reviews_dataset.csv
│   │   ├── olist_products_dataset.csv
│   │   ├── olist_sellers_dataset.csv
│   │   ├── olist_geolocation_dataset.csv
│   │   └── product_category_name_translation.csv
│   │
│   ├── bronze/                            # Bronze layer ingestion
│   │   └── bronze-ingestion-framework.ipynb  # Parameterized CSV → Delta ingestion
│   │
│   ├── silver/                            # Silver layer transformations
│   │   ├── configs/                       # JSON configuration files
│   │   │   ├── customers_config.json
│   │   │   ├── orders_config.json
│   │   │   ├── order_items_config.json
│   │   │   ├── order_payments_config.json
│   │   │   ├── order_reviews_config.json
│   │   │   ├── products_config.json
│   │   │   ├── sellers_config.json
│   │   │   ├── geolocation_config.json
│   │   │   └── prod_cat_nm_trans_config.json
│   │   │
│   │   ├── src/                           # Python transformation framework
│   │   │   ├── main.py                    # Pipeline orchestration
│   │   │   ├── readers/
│   │   │   │   └── bronze_reader.py       # Bronze table reader
│   │   │   ├── transformers/
│   │   │   │   ├── data_cleanser.py       # Null removal, deduplication, capitalization
│   │   │   │   ├── data_type_converter.py # Type casting (int, double, timestamp, etc.)
│   │   │   │   ├── derived_fields.py      # Extract date/time components
│   │   │   │   └── validator.py           # Data validation logic
│   │   │   └── writers/
│   │   │       └── silver_writer.py       # Delta table writer
│   │   │
│   │   └── silver_layer_pipeline.ipynb    # Orchestration notebook (optional)
│   │
│   └── gold/                              # Gold layer analytics
│       ├── total_revenue.ipynb            # Sum of all payments
│       ├── total_orders.ipynb             # Count of orders
│       ├── average_order_value.ipynb      # Average payment per order
│       ├── total_revenue_by_month.ipynb   # Monthly revenue trends
│       ├── average_delivery_time.ipynb    # Avg days from purchase to delivery
│       ├── repeat_customers.ipynb         # Customers with >1 order
│       ├── delayed_orders.ipynb           # Orders delivered late
│       ├── top_products.ipynb             # Best-selling products
│       ├── top_sellers.ipynb              # Top-performing sellers
│       └── top_cities.ipynb               # Cities with most orders
│
└── README.md                              # This file
```

---

## 🥉 Bronze Layer - Data Ingestion

### Purpose
Ingest raw CSV files into Delta Lake tables with **minimal transformation**.

### Framework: `bronze-ingestion-framework.ipynb`

#### Features
* **Parameterized ingestion** using Databricks widgets
* **CSV reading** with multiline support, quote handling, and whitespace trimming
* **Metadata enrichment**: Adds `ingestion_time` and `source` columns
* **Delta Lake storage**: ACID transactions, schema enforcement
* **Overwrite mode**: Full refresh on each run

#### Notebook Parameters (Widgets)
```python
* input_file_path      # Path to source CSV file
* target_catalog       # Unity Catalog name (e.g., "dev")
* target_schema        # Schema name (e.g., "bronze")
* target_table         # Table name (e.g., "bronze_customers")
```

#### Code Snippet
```python
def bronze_ingetion(input_file_path, output_file_path):
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("ignoreLeadingWhiteSpace", "true") \
        .option("ignoreTrailingWhiteSpace", "true") \
        .option("multiLine", "true") \
        .load(input_file_path)
    
    df = df.withColumn("ingestion_time", current_timestamp()) \
           .withColumn("source", lit(input_file_path))
    
    df.write.mode("overwrite").format("delta").saveAsTable(output_file_path)
```

#### Output Tables
```
dev.bronze.bronze_customers
dev.bronze.bronze_orders
dev.bronze.bronze_order_items
dev.bronze.bronze_order_payments
dev.bronze.bronze_order_reviews
dev.bronze.bronze_products
dev.bronze.bronze_sellers
dev.bronze.bronze_geolocation
dev.bronze.bronze_prod_cat_nm_trans
```

---

## 🥈 Silver Layer - Data Transformation

### Purpose
Transform bronze data into **clean, validated, business-ready** tables.

### Architecture: Config-Driven Python Framework

#### Main Pipeline: `src/main.py`

**Orchestrates** the following steps:
1. **Read** bronze table
2. **Remove nulls** (configurable columns)
3. **Drop duplicates** (all columns)
4. **Capitalize** column values (e.g., city names)
5. **Convert** data types (timestamps, dates, integers, doubles)
6. **Extract** derived date/time fields (year, month, day, hour, minute, second)
7. **Write** to silver Delta table

#### Configuration Files (`/silver/configs/*.json`)

Each source table has a JSON config defining transformations:

**Example: `customers_config.json`**
```json
{
  "source_table": "dev.bronze.bronze_customers",
  "target_table": "dev.silver.silver_customers",
  "primary_key": ["customer_id"],
  "watermark_column": "updated_at",
  "dedup_order_column": "updated_at",
  "required_columns": ["customer_id"],
  "capitalize_columns": ["customer_city"],
  "drop_duplicates": true,
  "remove_nulls": true,
  "audit_columns": true,
  "convert_columns": {},
  "derived_ts_columns": ["ingestion_time"]
}
```

**Example: `orders_config.json`**
```json
{
  "source_table": "dev.bronze.bronze_orders",
  "target_table": "dev.silver.silver_orders",
  "primary_key": ["order_id"],
  "required_columns": ["order_id", "customer_id"],
  "capitalize_columns": [],
  "drop_duplicates": true,
  "remove_nulls": true,
  "convert_columns": {
    "order_purchase_timestamp": "timestamp",
    "order_approved_at": "timestamp",
    "order_delivered_carrier_date": "timestamp",
    "order_delivered_customer_date": "timestamp",
    "order_estimated_delivery_date": "timestamp"
  },
  "derived_ts_columns": [
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
  ],
  "drop_null_columns": ["order_id", "customer_id"]
}
```

#### Transformation Modules

| Module | Class | Methods | Purpose |
| --- | --- | --- | --- |
| `bronze_reader.py` | BronzeReader | `read_bronze(table_name)` | Read bronze Delta table |
| `data_cleanser.py` | DataCleanser | `drop_nulls(df, cols)` | Remove rows with nulls |
|  |  | `drop_duplicates(df)` | Remove duplicate rows |
|  |  | `capitalized(df, cols)` | Capitalize column values (initcap) |
| `data_type_converter.py` | DataTypeConverter | `convert_columns(df, map)` | Cast multiple columns to target types |
| `derived_fields.py` | DerivedFields | `extract_dates_time(df, cols)` | Extract year, month, day, hour, min, sec |
| `silver_writer.py` | SilverWriter | `write_to_table(df, name)` | Write DataFrame to Delta table |

#### Running the Pipeline

**Command-line execution:**
```bash
python /Workspace/ecommerce-lakehouse/notebooks/silver/src/main.py \
  /Workspace/ecommerce-lakehouse/notebooks/silver/configs/customers_config.json
```

**Databricks notebook:**
```python
%run /ecommerce-lakehouse/notebooks/silver/src/main.py \
  /Workspace/ecommerce-lakehouse/notebooks/silver/configs/orders_config.json
```

#### Output Tables
```
dev.silver.silver_customers
dev.silver.silver_orders
dev.silver.silver_order_items
dev.silver.silver_order_payments
dev.silver.silver_order_reviews
dev.silver.silver_products
dev.silver.silver_sellers
dev.silver.silver_geolocation
dev.silver.silver_prod_cat_nm_trans
```

---

## 🥇 Gold Layer - Business Analytics

### Purpose
Create **aggregated, business-ready metrics** for dashboards and reporting.

### Analytics Notebooks

| Notebook | Description | Source Table(s) | Output Table |
| --- | --- | --- | --- |
| **total_revenue.ipynb** | Sum of all payment values | silver_order_payments | gold_total_revenue |
| **total_orders.ipynb** | Count of unique orders | silver_orders | gold_total_orders |
| **average_order_value.ipynb** | Average payment per order | silver_order_payments | gold_avg_order_value |
| **total_revenue_by_month.ipynb** | Monthly revenue aggregation | silver_order_payments, silver_orders | gold_revenue_by_month |
| **average_delivery_time.ipynb** | Avg days from purchase to delivery | silver_orders | gold_avg_delivery_time |
| **repeat_customers.ipynb** | Customers with multiple orders | silver_orders | gold_repeat_customers |
| **delayed_orders.ipynb** | Orders delivered after estimated date | silver_orders | gold_delayed_orders |
| **top_products.ipynb** | Best-selling products by quantity | silver_order_items, silver_products | gold_top_products |
| **top_sellers.ipynb** | Sellers by revenue/order count | silver_order_items, silver_sellers | gold_top_sellers |
| **top_cities.ipynb** | Cities by customer order frequency | silver_orders, silver_customers | gold_top_cities |

### Example: Total Revenue Calculation

**Notebook: `total_revenue.ipynb`**
```python
# Read and aggregate
order_payments = spark.sql("""
    SELECT SUM(payment_value) as total_revenue 
    FROM dev.silver.silver_order_payments
""")

# Write to gold table
order_payments.write.mode("overwrite").saveAsTable("dev.gold.gold_total_revenue")
```

### Example: Top Cities by Order Count

**Notebook: `top_cities.ipynb`**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, dense_rank, count
import pyspark.sql.functions as F

# Aggregate orders by customer
WindowSpec = Window.orderBy(F.col('order_count').desc())
orders = spark.read.table('dev.silver.silver_orders') \
    .groupBy('customer_id') \
    .agg(count('customer_id').alias('order_count')) \
    .withColumn('rnk', dense_rank().over(WindowSpec))

# Join with customer city information
customers = spark.read.table('dev.silver.silver_customers') \
    .select('customer_id', 'customer_city')

top_cities = orders \
    .join(customers, orders.customer_id == customers.customer_id, 'left') \
    .select(orders.customer_id, customers.customer_city, orders.order_count, orders.rnk)

# Write to gold table
top_cities.write.mode("overwrite").saveAsTable("dev.gold.gold_top_cities")
```

---

## ⚙️ Configuration Management

### Silver Layer Configuration Schema

Each transformation config supports:

| Parameter | Type | Description | Example |
| --- | --- | --- | --- |
| `source_table` | string | Bronze table path | `"dev.bronze.bronze_orders"` |
| `target_table` | string | Silver table path | `"dev.silver.silver_orders"` |
| `primary_key` | array | Unique identifier columns | `["order_id"]` |
| `watermark_column` | string | Column for incremental processing | `"updated_at"` |
| `dedup_order_column` | string | Column to determine latest record | `"updated_at"` |
| `required_columns` | array | Columns that must exist | `["order_id", "customer_id"]` |
| `capitalize_columns` | array | Columns to apply initcap() | `["customer_city"]` |
| `drop_duplicates` | boolean | Enable duplicate removal | `true` |
| `remove_nulls` | boolean | Enable null removal | `true` |
| `drop_null_columns` | array | Specific columns to check for nulls | `["order_id"]` |
| `convert_columns` | object | Data type conversions | `{"order_date": "timestamp"}` |
| `derived_ts_columns` | array | Timestamp cols to extract date parts | `["order_delivered_customer_date"]` |
| `audit_columns` | boolean | Add audit metadata | `true` |

---

## 🔄 Data Flow

### End-to-End Pipeline

```
1. CSV Files (raw-data/)
   ├─> Read via bronze-ingestion-framework.ipynb
   ├─> Add ingestion_time, source
   └─> Write to dev.bronze.bronze_* (Delta)

2. Bronze Tables
   ├─> Read via BronzeReader
   ├─> Apply transformations (config-driven)
   │   ├─> Drop nulls (DataCleanser)
   │   ├─> Drop duplicates (DataCleanser)
   │   ├─> Capitalize columns (DataCleanser)
   │   ├─> Convert data types (DataTypeConverter)
   │   └─> Extract date/time fields (DerivedFields)
   └─> Write to dev.silver.silver_* (Delta)

3. Silver Tables
   ├─> Read via spark.read.table()
   ├─> Apply business logic (SQL aggregations, joins, window functions)
   └─> Write to dev.gold.gold_* (Delta)

4. Gold Tables
   └─> Consumed by dashboards, BI tools, ML models
```

### Processing Modes

* **Bronze**: Full refresh (overwrite mode)
* **Silver**: Full refresh (overwrite mode) — can be extended to incremental
* **Gold**: Full refresh (overwrite mode) — can be scheduled for daily/hourly updates

---

## 🚀 How to Run

### Prerequisites
* Databricks workspace with Unity Catalog enabled
* Cluster with **Databricks Runtime 13.0+**
* CSV files uploaded to `/ecommerce-lakehouse/notebooks/raw-data/`
* Catalogs and schemas created:
  ```sql
  CREATE CATALOG IF NOT EXISTS dev;
  CREATE SCHEMA IF NOT EXISTS dev.bronze;
  CREATE SCHEMA IF NOT EXISTS dev.silver;
  CREATE SCHEMA IF NOT EXISTS dev.gold;
  ```

### Step 1: Bronze Layer Ingestion

**Run the `bronze-ingestion-framework.ipynb` notebook for each CSV file:**

**Example: Ingest customers data**
```python
# Set widget parameters
dbutils.widgets.text("input_file_path", "/Workspace/ecommerce-lakehouse/notebooks/raw-data/olist_customers_dataset.csv")
dbutils.widgets.text("target_catalog", "dev")
dbutils.widgets.text("target_schema", "bronze")
dbutils.widgets.text("target_table", "bronze_customers")

# Run the notebook
%run ./bronze-ingestion-framework
```

**Repeat for all 9 CSV files** (customers, orders, order_items, order_payments, order_reviews, products, sellers, geolocation, product_category_name_translation)

### Step 2: Silver Layer Transformation

**Run the `main.py` script for each config file:**

```bash
# From Databricks notebook:
%run /ecommerce-lakehouse/notebooks/silver/src/main.py \
  /Workspace/ecommerce-lakehouse/notebooks/silver/configs/customers_config.json

%run /ecommerce-lakehouse/notebooks/silver/src/main.py \
  /Workspace/ecommerce-lakehouse/notebooks/silver/configs/orders_config.json

# ... repeat for all 9 config files
```

**Or use Databricks CLI:**
```bash
databricks jobs create --json '{
  "name": "Silver Layer Pipeline",
  "tasks": [
    {
      "task_key": "transform_customers",
      "python_script_task": {
        "python_file": "/Workspace/ecommerce-lakehouse/notebooks/silver/src/main.py",
        "parameters": ["/Workspace/ecommerce-lakehouse/notebooks/silver/configs/customers_config.json"]
      }
    }
  ]
}'
```

### Step 3: Gold Layer Analytics

**Run each gold layer notebook individually:**

```python
# Example: Run total_revenue notebook
%run /ecommerce-lakehouse/notebooks/gold/total_revenue

# Example: Run average_order_value notebook
%run /ecommerce-lakehouse/notebooks/gold/average_order_value

# ... repeat for all 10 analytics notebooks
```

### Step 4: Query Gold Tables

```sql
-- Total revenue
SELECT * FROM dev.gold.gold_total_revenue;

-- Average order value
SELECT * FROM dev.gold.gold_avg_order_value;

-- Top 10 cities by order count
SELECT customer_city, SUM(order_count) as total_orders
FROM dev.gold.gold_top_cities
GROUP BY customer_city
ORDER BY total_orders DESC
LIMIT 10;

-- Monthly revenue trend
SELECT * FROM dev.gold.gold_revenue_by_month
ORDER BY month;
```

---

## 🛠️ Technologies Used

| Technology | Purpose | Version |
| --- | --- | --- |
| **Databricks** | Unified data analytics platform | Workspace |
| **Apache Spark** | Distributed data processing | 3.x |
| **Delta Lake** | ACID transactions, time travel | 2.x |
| **Unity Catalog** | Unified governance (catalog.schema.table) | Enabled |
| **PySpark** | Python API for Spark | 3.x |
| **Python** | Transformation framework | 3.10+ |
| **JSON** | Configuration files | - |
| **SQL** | Analytical queries | Databricks SQL |
| **Jupyter Notebooks** | Interactive development | Databricks Notebooks |

---

## 📊 Data Models

### Bronze Layer Schema

All bronze tables include **metadata columns**:
* `ingestion_time` (timestamp): When data was loaded
* `source` (string): Source file path

### Silver Layer Schema Examples

**`dev.silver.silver_orders`**
```
order_id                             string
customer_id                          string
order_status                         string
order_purchase_timestamp             timestamp
order_approved_at                    timestamp
order_delivered_carrier_date         timestamp
order_delivered_customer_date        timestamp
order_estimated_delivery_date        timestamp
order_delivered_customer_date_year   int
order_delivered_customer_date_month  int
order_delivered_customer_date_day    int
order_delivered_customer_date_hour   int
order_delivered_customer_date_minute int
order_delivered_customer_date_second int
(+ similar fields for estimated_delivery_date)
ingestion_time                       timestamp
source                               string
```

**`dev.silver.silver_customers`**
```
customer_id            string
customer_unique_id     string
customer_zip_code      string
customer_city          string (capitalized)
customer_state         string
ingestion_time         timestamp
source                 string
```

### Gold Layer Schema Examples

**`dev.gold.gold_total_revenue`**
```
total_revenue  double
```

**`dev.gold.gold_top_cities`**
```
customer_id    string
customer_city  string
order_count    bigint
rnk            int
```

---

## 📈 Key Metrics & KPIs

The gold layer provides the following business metrics:

### Revenue Metrics
* **Total Revenue**: Sum of all payments
* **Average Order Value**: Total revenue ÷ number of orders
* **Revenue by Month**: Time-series monthly aggregation

### Order Metrics
* **Total Orders**: Count of unique order_id
* **Delayed Orders**: Orders delivered after estimated date
* **Average Delivery Time**: Avg days from purchase to delivery

### Customer Metrics
* **Repeat Customers**: Customers with >1 order
* **Top Cities**: Cities ranked by order frequency

### Product & Seller Metrics
* **Top Products**: Products by sales volume
* **Top Sellers**: Sellers by revenue/order count

---

## 🔮 Future Enhancements

### Incremental Processing
* Implement **Delta Lake Merge** for incremental silver layer updates
* Add **watermark-based** change data capture (CDC)
* Use **OPTIMIZE** and **Z-ORDERING** for query performance

### Data Quality
* Add **expectations framework** (Great Expectations)
* Implement **data validation rules** in silver layer
* Create **data quality dashboards**

### Orchestration
* Create **Databricks Workflows** (Jobs) for end-to-end automation
* Schedule bronze ingestion (daily)
* Schedule silver transformations (hourly)
* Schedule gold refreshes (real-time or hourly)

### Advanced Analytics
* Customer segmentation (RFM analysis)
* Product recommendation engine
* Churn prediction models (MLflow)
* Real-time dashboards (Lakeview)

### Monitoring & Observability
* Add **logging** to all pipelines
* Implement **alerting** for pipeline failures
* Track **data lineage** with Unity Catalog
* Monitor **table statistics** and query performance

---

## 📝 Notes

* **All tables use Delta Lake format** for ACID compliance
* **Unity Catalog** enforces governance (catalog.schema.table)
* **Config-driven approach** makes silver layer extensible
* **Medallion architecture** separates concerns (raw → clean → business)
* **Overwrite mode** currently used (can be changed to append/merge)
* **No incremental processing** yet (full refreshes on each run)

---

## 🤝 Contributing

To extend this project:
1. Add new CSV files to `/raw-data/`
2. Run bronze ingestion with new parameters
3. Create new config JSON in `/silver/configs/`
4. Add transformation logic in `/silver/src/transformers/` if needed
5. Run silver pipeline with new config
6. Create new gold analytics notebooks

---

## 📧 Contact

For questions or issues, contact the data engineering team.

---

**Last Updated**: June 2026  
**Version**: 1.0  
**Maintained By**: Data Engineering Team