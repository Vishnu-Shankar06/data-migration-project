import requests
from logger import log_error

API_URL = "http://127.0.0.1:8000/customers"

def push_to_api(row):
    try:
        res = requests.post(API_URL, json=row, timeout=5)

        if res.status_code in [200, 201]:
            return True

        log_error(f"API failed {res.status_code} {row}")
        return False

    except Exception as e:
        log_error(f"Exception {str(e)} {row}")
        return False