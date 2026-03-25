from fastapi import FastAPI

app = FastAPI()

db = []

@app.get("/customers")
def get_customers():
    return db

@app.post("/customers")
def create_customer(data: dict):
    db.append(data)
    print(f"API Received: {data['name']}") 
    return {"status": "success"}

@app.get("/customers/{id}")
def get_customer(id: int):
    for c in db:
        if c["customer_id"] == id:
            return c
    return {"error": "Not found"}