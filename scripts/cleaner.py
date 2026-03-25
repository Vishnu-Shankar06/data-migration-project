from datetime import datetime

def clean_customer_row(row):
    name = row.get("name", "").strip()
# Name is required for readability, fallback to placeholder instead of removing row
    row["name"] = name.title() if name else "None"

    email = row.get("email", "").strip().lower()
# Emails without '@' are considered invalid → replace with "None"
    if email and "@" in email:
        row["email"] = email
    else:
        row["email"] = "None"

    plan = row.get("subscription", "").strip().lower()
    row["subscription"] = plan if plan else "None"

    raw_price = str(row.get("price", "")).strip().lower()
    if raw_price in ["free", "zero"]:
# If price is "free" or invalid, defaulting to 0 instead of dropping the row
# because downstream systems expect numeric values
        row["price"] = 0.0
    elif not raw_price:
        row["price"] = "None"
    else:
        try:
            price_val = float(raw_price)
# Negative prices don't make sense → mark as "None" instead of keeping bad data
            row["price"] = price_val if price_val >= 0 else "None"
        except ValueError:
            row["price"] = "None"

    raw_date = row.get("start_date", "").strip()
    if not raw_date:
        row["start_date"] = "None"
    else:
        try:
            valid_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
            row["start_date"] = str(valid_date)
        except ValueError:
            try:
                valid_date = datetime.strptime(raw_date, "%m/%d/%Y").date()
                row["start_date"] = str(valid_date)
            except ValueError:
                try:
                    valid_date = datetime.strptime(raw_date, "%m-%d-%Y").date()
                    row["start_date"] = str(valid_date)
                except ValueError:
                    row["start_date"] = "None"

    return row