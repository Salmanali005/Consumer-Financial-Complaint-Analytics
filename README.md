# Consumer Financial Complaint Analytics Lakehouse

An end-to-end data lakehouse pipeline built on Apache Spark and Delta Lake using the Medallion Architecture (Bronze, Silver, Gold) to ingest real consumer complaint data from the CFPB, validate and deduplicate records, and serve audit logs and analytical models.

---

## Project Overview
- **Domain:** Financial Services / Consumer Complaint & Risk Analytics
- **Data Source:** CFPB Consumer Complaint Database API — real, live, publicly available complaint records against financial companies, fetched using Elasticsearch `search_after` cursor pagination.
- **Platform Stack:** Databricks (Apache Spark), Delta Lake, Python / PySpark, Power BI
- **Course:** Data Analysis and Visualization — Semester Project (Phase 2 Complete)
- **Project Team:**
  - Salman Ali — Roll Number: 24L2542
  - Ahmad Khan — Roll Number: 24L2541

---

## Why This Data Source
Unlike a static synthetic dataset, the CFPB API provides genuinely live data: a Full Load is pulled as a historical baseline over a fixed date range, and an Incremental Load is pulled separately for complaints received after that cutoff — mirroring how a real production pipeline ingests new records as they arrive.

---

## Architecture: Medallion Pipeline
The lakehouse pipeline processes complaint data across distinct, fault-tolerant tiers:

    Raw Data (CFPB API — JSON via search_after)
              │
              ▼
┌────────────────────────────────────────────────────────┐
│             Bronze Layer (Delta Lake)                  │  <-- Raw Ingestion + Audit Metadata
│  - Partitioned by run_date with idempotent overwrite   │      (load_timestamp, batch_id, run_date)
└────────────────────┬───────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────┐
│             Silver Layer (Delta Lake)                  │  <-- Cleansed & Standardized
│  - Try-to-timestamp date parsing & type casting        │  - Duplicate Complaint Removal (Window)
│  - Quarantine routing for malformed records            │  - Idempotent MERGE INTO deduplication
└────────────────────┬───────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────┐
│             Audit Logging & Orchestration              │
│  - pipeline_execution_logs tracking execution metrics  │
└────────────────────────────────────────────────────────┘

---

## Data Models & Schemas

### 1. Bronze Table (`workspace.complaints.bronze_complaints`)
- **Description:** Raw ingestion layer storing unvalidated complaint data as strings, partitioned by ingestion date (`run_date`).
- **Primary Key:** None (Append / Partition Overwrite layer)

| Column Name | Data Type | Nullable | Description |
|---|---|---|---|
| `complaint_id` | string | yes | Raw complaint identifier |
| `date_received` | string | yes | Raw date complaint was received |
| `product` | string | yes | Financial product category |
| `sub_product` | string | yes | Sub-product classification |
| `issue` | string | yes | Primary issue reported |
| `sub_issue` | string | yes | Secondary issue reported |
| `company` | string | yes | Target company name |
| `state` | string | yes | State abbreviation |
| `zip_code` | string | yes | Postal code |
| `submitted_via` | string | yes | Intake channel |
| `date_sent_to_company` | string | yes | Raw date forwarded to company |
| `company_response` | string | yes | Company response status |
| `timely` | string | yes | Timeliness indicator flag |
| `company_public_response` | string | yes | Public response text |
| `tags` | string | yes | Additional complaint tags |
| `load_timestamp` | timestamp | **no** | Timestamp when ingested into Bronze |
| `source_file` | string | yes | Source CSV/API batch file name |
| `batch_id` | string | yes | Unique batch execution ID |
| `load_type` | string | yes | Ingestion mode (`incremental` / `full`) |
| `run_date` | date | **no** | Partition date for idempotent overwrites |

---

### 2. Silver Table (`workspace.complaints.silver_complaints`)
- **Description:** Cleaned, validated, and type-cast analytical table with strict primary key constraints and idempotent deduplication via `MERGE INTO`.
- **Primary Key:** `complaint_id`

| Column Name | Data Type | Nullable | Key | Description |
|---|---|---|---|---|
| `complaint_id` | string | **no** | **PK** | Unique complaint identifier |
| `date_received` | timestamp | yes | | Parsed timestamp of receipt |
| `date_sent_to_company` | timestamp | yes | | Parsed timestamp forwarded to company |
| `product` | string | yes | | Normalized product category |
| `sub_product` | string | yes | | Normalized sub-product |
| `issue` | string | yes | | Primary issue |
| `sub_issue` | string | yes | | Secondary issue |
| `company` | string | yes | | Company name |
| `state` | string | yes | | State code |
| `zip_code` | string | yes | | Postal code |
| `submitted_via` | string | yes | Intake channel |
| `company_response` | string | yes | Company response status |
| `company_public_response` | string | yes | Public response |
| `timely` | boolean | yes | Timeliness boolean flag |
| `tags` | string | yes | Complaint tags |
| `load_timestamp` | timestamp | **no** | Timestamp of last load/refresh |
| `batch_id` | string | yes | Batch identifier |

---

## Pipeline Run Guide

### 1. Running a Normal Incremental Load
1. Open **`Complaint-Analytics-Pipeline.ipynb`** (Master Orchestrator).
2. Set execution parameters:
   - **`mode`**: `auto` (automatically detects watermark state and pulls lagging completed days).
3. Click **Run All**. The orchestrator executes API extraction via `search_after`, loads Bronze via partition overwrite (`replaceWhere`), and merges cleanly into Silver.

### 2. Running a Historical Backfill
1. Open **`Complaint-Analytics-Pipeline.ipynb`**.
2. Set execution parameters:
   - **`mode`**: `manual`
   - **`start_date`**: `2026-10-01`
   - **`end_date`**: `2026-10-01`
3. Click **Run All**. The pipeline fetches historical windows, validates date types safely using `try_to_timestamp`, routes invalid records to `silver_quarantine`, and merges clean records without duplication.

---

## Repository Structure
    ├── README.md
    ├── .gitignore
    ├── data/
    │   └── samples/
    │       ├── full_load_sample.csv
    │       └── incremental_load_sample.csv
    └── Notebooks/
        ├── 01_Setup.ipynb
        ├── 02_ingest_api.ipynb
        ├── 03_raw_to_bronze.ipynb
        ├── 04_bronze_to_silver.ipynb
        └── Complaint-Analytics-Pipeline.ipynb

---

## Security & Privacy
The CFPB dataset does not include customer names, account numbers, or transaction amounts. It contains only complaint metadata (company, product, state, dates, response status), so no hashing or masking of direct personal identifiers is required.

