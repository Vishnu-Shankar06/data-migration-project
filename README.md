# 🚀 Enterprise Billing Data Migration Pipeline (ETL)

## 🎯 Executive Summary
During B2B SaaS onboarding, migrating legacy billing data (from tools like Stripe, Chargebee, or unstructured spreadsheets) is often the highest-friction step. Client data is notoriously messy, filled with missing fields, duplicate records, and inconsistent formats.

This project is a production-grade **Extract, Transform, Load (ETL) Pipeline** built in Python. It simulates a robust customer onboarding flow that ingests highly unstructured CSV data, resolves duplicates, normalizes the formatting, and safely upserts the payload into both a local relational database and a remote REST API destination.

---

## 🧠 Core Engineering Principles

To ensure production readiness, this pipeline was designed around four core data engineering philosophies:

### 1. Zero Data Loss (100% Retention Policy)
In billing migrations, dropping a customer record due to a missing email is a critical business failure. This pipeline implements a **"Fix or Flag"** philosophy. Instead of rejecting rows with bad data, it standardizes what it can and explicitly labels unfixable fields (e.g., negative prices, unreadable dates) as `"None"`. 
* *Result: 100% of the client's customer base is migrated and accounted for.*

### 2. Medallion Architecture (Bronze & Gold Layers)
Data is stored locally in an SQLite database using a multi-tiered approach:
* **Raw Table (Bronze Layer):** The exact, untouched string data from the CSV is backed up here first. This ensures total traceability if the client audits the migration.
* **Cleaned Table (Gold Layer):** The sanitized, strictly-typed data is stored here after successful transformation and API integration.

### 3. Idempotency & State Management
Running this script multiple times will never result in database crashes or duplicate API entries. 
* Database queries utilize `INSERT OR REPLACE` (Upsert) logic.
* If the pipeline is interrupted and restarted, it gracefully overwrites the existing state without violating Primary Key constraints.

### 4. Automated Deduplication (Last-Write-Wins)
Legacy exports often contain multiple chronological entries for the same customer. The pipeline intercepts the raw data stream and applies a dictionary-based deduplication strategy, ensuring only the most recent state of a `customer_id` is processed and migrated.

---

## 🏗️ System Architecture & Data Flow

1. **Extraction:** Reads `raw.csv` and deduplicates records into memory.
2. **Bronze Storage:** Upserts the raw text data into the `raw_customers` SQLite table.
3. **Transformation (`cleaner.py`):**
   * Normalizes capitalization (Title Case for names).
   * Validates email structures (checks for `@`).
   * Handles string-to-float conversions for pricing (e.g., converts `"free"` to `0.0`, flags negatives).
   * Parses multiple date formats (`YYYY-MM-DD`, `MM/DD/YYYY`, `MM-DD-YYYY`) into ISO standards.
4. **Validation (`validator.py`):** Acts as a pass-through gatekeeper (configured to support the 100% retention rule).
5. **API Integration (`uploader.py`):** Pushes the transformed JSON payload via HTTP POST to the destination server with built-in timeout handling.
6. **Gold Storage:** Upserts the finalized records into the `cleaned_customers` SQLite table.
7. **Error Handling (`logger.py`):** Silent, non-blocking error logging ensures one bad network request doesn't crash the entire migration.

---

## 💻 Tech Stack

* **Language:** Python 3.x
* **Data Processing:** Native Python (`csv`, `datetime`)
* **Database:** SQLite3
* **Network/API:** `requests`
* **Mock Server:** `FastAPI`, `Uvicorn`

---

## ⚙️ Local Setup & Execution

### 1. Install Dependencies
Create a virtual environment and install the required packages:

```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Start the Mock Destination API
Open a terminal and start the FastAPI server. This acts as the external SaaS platform receiving the migrated data.

```bash
uvicorn main:app --reload
```
*The API will be live at `http://127.0.0.1:8000/customers`*

### 3. Run the ETL Pipeline
Open a **second** terminal window and execute the migration script:

```bash
python migrate.py
```

---

## 📊 Sample Execution Output

When running the pipeline against a highly corrupted 100-row CSV file with duplicate IDs and missing fields, the system successfully deduplicates and recovers the data:

```text
$ python migrate.py
[INFO] Found 100 unique customer IDs
[PROCESSING] Customer ID: 1
[PROCESSING] Customer ID: 2
...
[PROCESSING] Customer ID: 100

Pipeline Complete!
Total Unique IDs processed: 100
Raw records stored: 100
API uploads successful: 100
Clean records stored: 100
```

---

## 🚀 Future Enhancements (Roadmap)
While currently production-ready for standard CSVs, future iterations for enterprise scaling will include:
* **Batch Processing:** Transitioning from per-row API calls to batch payloads (`POST /customers/batch`) to reduce network overhead.
* **Exponential Backoff:** Implementing a retry mechanism for API rate limits (HTTP 429) or transient network failures.
* **Memory Optimization:** Replacing the in-memory dictionary deduplication with a temporary SQLite staging table for massive datasets (>5GB) that exceed RAM limits.
