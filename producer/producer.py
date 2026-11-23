import json, random, time, uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

fake = Faker()
producer = Producer({"bootstrap.servers": "localhost:9092"})
CATEGORIES = ["electronics", "fashion", "grocery", "books", "toys"]

def make_order():
    return {
        "order_id": str(uuid.uuid4()),
        "user": fake.user_name(),
        "category": random.choice(CATEGORIES),
        "amount": round(random.uniform(5, 500), 2),
        "city": fake.city(),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }

if __name__ == "__main__":
    while True:
        order = make_order()
        producer.produce("orders", key=order["category"], value=json.dumps(order))
        producer.poll(0)
        print("sent:", order["order_id"], order["category"], order["amount"])
        time.sleep(random.uniform(0.2, 1.0))