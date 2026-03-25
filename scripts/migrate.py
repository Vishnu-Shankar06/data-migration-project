import sqlite3
import csv
from cleaner import clean_customer_row
from validator import validate_customer
from uploader import push_to_api
from logger import log_error
import os

# Establish DB connection
os.makedirs("output", exist_ok=True)
conn = sqlite3.connect("output/data.db")
cursor = conn.cursor()

# Create tables if not present
# Keeping raw and cleaned separately to allow debugging and traceability
cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_customers (
        customer_id INTEGER PRIMARY KEY,
        name TEXT,
        email TEXT,
        subscription TEXT,
        price TEXT,
        start_date TEXT
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS cleaned_customers (
        customer_id INTEGER PRIMARY KEY,
        name TEXT,
        email TEXT,
        subscription TEXT,
        price REAL,
        start_date TEXT
    )
""")

conn.commit()

success_raw_db = 0
success_clean_db = 0
success_api = 0

# We only keep the latest occurrence of each customer_id
# This avoids duplicate inserts and mimics "last update wins" behavior
latest_rows = {}

with open("data/raw.csv", newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)

    for row in reader:
        cid = row.get("customer_id")

        if cid:
            # Overwrite → ensures last occurrence is kept
            latest_rows[cid] = row
        else:
            # Edge case: missing customer_id
            # Not ideal, but skipping for now instead of crashing pipeline
            log_error(f"Missing customer_id in row: {row}")

print(f"[INFO] Found {len(latest_rows)} unique customer IDs")

# Process each unique row
for cid, row in latest_rows.items():

    print(f"[PROCESSING] Customer ID: {cid}")

    # Step 1: Store raw data (for audit/debug purposes)
    try:
        cursor.execute("""
        INSERT OR REPLACE INTO raw_customers 
        (customer_id, name, email, subscription, price, start_date) 
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            row.get("customer_id"),
            row.get("name"),
            row.get("email"),
            row.get("subscription"),
            row.get("price"),
            row.get("start_date")
        ))
        success_raw_db += 1
    except Exception as e:
        log_error(f"Failed RAW insert for ID {cid}: {str(e)}")

    # Step 2: Clean data
    # NOTE: We prefer fixing bad data instead of dropping it
    cleaned = clean_customer_row(row.copy())

    # Step 3: Validate (light validation, not strict rejection)
    is_valid, error_msg = validate_customer(cleaned)

    if not is_valid:
        # Logging instead of failing hard → pipeline should continue
        log_error(f"Validation issue [{error_msg}] for ID {cid}: {cleaned}")
        continue

    # Step 4: Push to API (simulating downstream system)
    api_success = push_to_api(cleaned)
    if api_success:
        success_api += 1
    else:
        log_error(f"API push failed for ID {cid}")

    # Step 5: Store cleaned data
    try:
        cursor.execute("""
        INSERT OR REPLACE INTO cleaned_customers 
        (customer_id, name, email, subscription, price, start_date) 
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            cleaned["customer_id"],
            cleaned["name"],
            cleaned["email"],
            cleaned["subscription"],
            cleaned["price"],
            cleaned["start_date"]
        ))
        success_clean_db += 1
    except Exception as e:
        log_error(f"Clean DB insert failed for ID {cid}: {str(e)}")

# Final commit
conn.commit()
conn.close()

print("\nPipeline Complete!")
print(f"Total Unique IDs processed: {len(latest_rows)}")
print(f"Raw records stored: {success_raw_db}")
print(f"API uploads successful: {success_api}")
print(f"Clean records stored: {success_clean_db}")

# TODO:
# - Improve duplicate handling (currently last-write-wins)
# - Add batch API upload instead of per-row calls
# - Add retry mechanism for failed API calls