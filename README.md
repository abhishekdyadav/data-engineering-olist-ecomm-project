# 🛒 Olist E-Commerce — End-to-End Azure Data Engineering Pipeline

![Azure Data Factory](https://img.shields.io/badge/Azure-Data%20Factory-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Azure Databricks](https://img.shields.io/badge/Azure-Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Azure Synapse](https://img.shields.io/badge/Azure-Synapse%20Analytics-8661C5?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Azure ADLS](https://img.shields.io/badge/Azure-Data%20Lake%20Storage%20Gen2-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Status](https://img.shields.io/badge/Status-Active-success?style=for-the-badge)

---

## 📋 Overview

An end-to-end cloud data engineering pipeline built entirely on **Microsoft Azure**, processing the Brazilian **Olist E-Commerce** public dataset. The pipeline ingests data from multiple heterogeneous sources, transforms it using distributed PySpark processing, and serves business-ready data through Synapse Analytics for downstream consumption.

The project follows **Medallion Architecture** (Bronze → Silver → Gold) and simulates a real-world production data engineering environment with live database sources, dynamic orchestration, and layered data quality.

---

## 🏗️ Architecture

![Architecture](architecture/pipeline_architecture.png)

The pipeline follows **Medallion Architecture** across three layers:

- **Bronze** — Azure Data Factory ingests raw CSV files from GitHub via a dynamic Lookup + ForEach pipeline, and pulls live data from MySQL. All raw files land in ADLS Gen2 as-is.
- **Silver** — Azure Databricks reads the raw Bronze files, joins all 7 tables using PySpark, removes duplicate columns, and writes a single clean Parquet file back to ADLS Gen2.
- **Gold** — Azure Synapse Analytics serverless SQL reads the Silver Parquet via OPENROWSET, applies business logic through views, and persists the final output as an external table in ADLS Gen2 using CETAS.

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Orchestration | Azure Data Factory | Pipeline scheduling, dynamic ingestion |
| Storage | Azure Data Lake Storage Gen2 | Bronze / Silver / Gold data lake |
| Transformation | Azure Databricks (PySpark) | Distributed data processing and joins |
| Serving | Azure Synapse Analytics (Serverless SQL) | Business views and external tables |
| Source — Files | GitHub (HTTP connector) | 7 Olist CSV datasets |
| Source — Relational | MySQL on filess.io | Live production-simulated database |
| Source — NoSQL | MongoDB on filess.io | Live production-simulated database |
| Data Ingestion | Google Colab | Uploaded Olist data to MySQL and MongoDB |
| Authentication | App Registration + OAuth 2.0 | Databricks → ADLS secure access |
| Secret Management | Databricks Secret Scope | Client secrets never hardcoded |
| File Format | Parquet + Snappy Compression | Silver and Gold layers |

---

## 📂 Repository Structure

```
olist-azure-etl-pipeline/
│
├── README.md                              ← Project documentation
│
├── architecture/
│   └── pipeline_architecture.png         ← Full architecture diagram
│
├── adf/
│   ├── pipeline/
│   │   └── data_ingestion_pipeline.json  ← ADF pipeline definition
│   ├── dataset/
│   │   ├── CSVtoADLS.json                ← ADLS sink dataset
│   │   ├── DataFromGithubViaLinkedService.json ← HTTP source dataset
│   │   └── MySqlTable1.json              ← MySQL source dataset
│   └── trigger/
│       └── DailyTrigger.json             ← Daily schedule trigger
│
├── databricks/
│   └── transformation_notebook.ipynb     ← PySpark transformation notebook
│
├── synapse/
│   └── gold_layer_queries.sql            ← Gold layer SQL scripts
│
├── data_ingestion/
│   └── Data_Ingestion_to_SQL_DB.ipynb    ← Google Colab ingestion notebook
│
├── screenshots/
│   ├── adf_pipeline.png                  ← ADF pipeline canvas
│   ├── databricks_output.png             ← Databricks transformation output
│   └── synapse_external_table.png        ← Synapse gold layer output
│
└── .gitignore                            ← Excludes credentials and ARM templates
```

---

## 📊 Dataset

**Brazilian E-Commerce Public Dataset by Olist**

| Table | Description | Rows (approx) |
|---|---|---|
| olist_customers | Customer details and location | 99,441 |
| olist_orders | Order status and timestamps | 99,441 |
| olist_order_items | Items, price, freight per order | 112,650 |
| olist_products | Product details and dimensions | 32,951 |
| olist_sellers | Seller location details | 3,095 |
| olist_geolocation | Brazilian zip code coordinates | 1,000,163 |
| olist_order_reviews | Customer review scores | 99,224 |

---

## 🔄 Pipeline Details

### 🥉 Bronze Layer — Raw Ingestion (ADF)

- **Dynamic pipeline** using Lookup + ForEach pattern
- Lookup reads a JSON config file listing all 7 CSV files
- ForEach iterates over each file dynamically using `@item().csv_relative_url` and `@item().file_name`
- No hardcoded file names — adding a new file only requires updating the JSON config
- MySQL source ingested via native ADF MySQL connector
- **Error handling** — Web Activity alerts on failure for every activity
- **Daily trigger** configured for automated runs

```json
// Dynamic config JSON (Lookup source)
[
  {"csv_relative_url": "data/.../olist_customers_dataset.csv", "file_name": "olist_customers_dataset.csv"},
  {"csv_relative_url": "data/.../olist_orders_dataset.csv",    "file_name": "olist_orders_dataset.csv"}
  // ... 5 more files
]
```

### 🥈 Silver Layer — Transformation (Databricks)

- Reads all 7 CSVs from Bronze layer using `abfss://` protocol
- Performs multi-table **PySpark joins** across all 7 datasets
- Applies `remove_duplicate_columns()` custom function
- Authenticates to ADLS via **App Registration (OAuth 2.0)**
- Client secret stored securely in **Databricks Secret Scope**
- Writes final joined DataFrame as **Parquet** to Silver layer

```python
# Secure authentication pattern
client_secret = dbutils.secrets.get(scope="adls-scope", key="adls-client-secret")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}...", client_secret)
```

### 🥇 Gold Layer — Serving (Synapse Analytics)

- **Serverless SQL pool** — no dedicated compute cost
- `OPENROWSET` reads Silver Parquet directly without data movement
- Business views created in `gold` schema
- `CETAS` (Create External Table As Select) writes Gold data to ADLS
- **Managed Identity** authentication to ADLS

---

## 📈 Gold Layer Business Views

| View | Business Question Answered |
|---|---|
| `gold.final` | Row-level order details with delivery status flags and total order value |
| `gold.late_deliveries` | Late delivery breakdown by severity, seller location, and category |
| `gold.finaltable` | External table in ADLS Gold layer — served via Synapse SQL |

### Sample Business Insight Query

```sql
-- Category-wise Delivery Performance
SELECT
    category,
    COUNT(*) AS total_orders,
    ROUND(100.0 * SUM(CASE WHEN delivery_status = 'Early'   THEN 1 ELSE 0 END) / COUNT(*), 2) AS early_pct,
    ROUND(100.0 * SUM(CASE WHEN delivery_status = 'On Time' THEN 1 ELSE 0 END) / COUNT(*), 2) AS ontime_pct,
    ROUND(100.0 * SUM(CASE WHEN delivery_status = 'Late'    THEN 1 ELSE 0 END) / COUNT(*), 2) AS late_pct
FROM gold.final
GROUP BY category
HAVING COUNT(*) > 50
ORDER BY late_pct DESC;
```

---

## 🔐 Security

| Area | Implementation |
|---|---|
| Databricks → ADLS | App Registration + OAuth 2.0 (Service Principal) |
| Client Secret | Databricks Secret Scope (`dbutils.secrets.get()`) |
| Synapse → ADLS | Managed Identity (no passwords) |
| GitHub | Credentials replaced with placeholders — never committed |
| ARM Templates | Excluded via `.gitignore` |

---

## 🚀 Setup Instructions

### Prerequisites
- Azure Subscription
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks Workspace
- Azure Synapse Analytics Workspace
- GitHub Account

### Step 1 — Clone the Repo
```bash
git clone https://github.com/yourusername/olist-azure-etl-pipeline.git
```

### Step 2 — Create Azure Resources
```
├── Resource Group: your-resource-group
├── ADLS Gen2 Storage Account
│   └── Container: olistdata
│       ├── bronze/
│       ├── silver/
│       └── gold/
├── Azure Data Factory
├── Azure Databricks Workspace
└── Azure Synapse Analytics Workspace
```

### Step 3 — Configure App Registration
```
1. Azure Portal → App Registrations → New Registration
2. Note: Application (client) ID and Directory (tenant) ID
3. Certificates & Secrets → New Client Secret
4. Assign Storage Blob Data Contributor role on ADLS
```

### Step 4 — Set Up Databricks Secret Scope
```bash
# Install Databricks CLI
pip install databricks-cli

# Configure
databricks configure --token

# Add secrets
databricks secrets create-scope --scope adls-scope
databricks secrets put --scope adls-scope --key adls-client-secret
databricks secrets put --scope adls-scope --key application-id
databricks secrets put --scope adls-scope --key directory-id
```

### Step 5 — Import ADF Pipeline
```
1. Open ADF Studio
2. Manage → Git Configuration → Connect to this repo
3. Pipeline JSON auto-imports from adf/ folder
4. Configure Linked Services with your credentials
5. Publish
```

### Step 6 — Run Databricks Notebook
```
1. Upload transformation_notebook.ipynb to Databricks
2. Update storage_account name
3. Secrets load automatically from Secret Scope
4. Run All cells
```

### Step 7 — Execute Synapse SQL
```
1. Open Synapse Studio → Develop → New SQL Script
2. Paste gold_layer_queries.sql
3. Execute step by step
4. Verify gold.final and gold.finaltable
```

---

## 📸 Screenshots

### ADF Pipeline
![ADF Pipeline](screenshots/adf_pipeline.png)

### Databricks Transformation Output
![Databricks](screenshots/databricks_output.png)

### Synapse Gold Layer
![Synapse](screenshots/synapse_external_table.png)

---

## 🎯 Key Engineering Decisions

| Decision | Rationale |
|---|---|
| Medallion Architecture | Clear separation of raw, cleaned, and business-ready data |
| Dynamic ADF pipeline | No hardcoded file names — scales to any number of files |
| Parquet + Snappy | Columnar format reduces storage and speeds up analytical queries |
| Serverless Synapse SQL | No dedicated pool cost — pay per query for serving layer |
| OPENROWSET + CETAS | Reads Silver in-place, writes Gold without data duplication |
| App Registration + OAuth | Industry standard secure auth between Azure services |
| Secret Scope | Zero secrets in code — production security standard |

---

## 📝 Author

**Abhishek Yadav** :
Data Engineer | Azure | PySpark | SQL | Python

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=flat&logo=linkedin)](https://www.linkedin.com/in/abhishekdyadav/)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=flat&logo=github)](https://github.com/yourusername)
