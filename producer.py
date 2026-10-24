from confluent_kafka import Producer
import json
import uuid

# Fix 1: "bootstrap.servers" (plural)
producer_config = {
    "bootstrap.servers": "localhost:9092"
}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivery succeeded: {msg.topic()} [{msg.partition()}]")

order = {
    "order_id": str(uuid.uuid4()),
    "user": "lara",
    "item": "frozen yoghurt",
    "quantity": 10
}
value = json.dumps(order).encode("utf-8")

# Fix 2: "topic" keyword argument
producer.produce(
    topic="orders",
    value=value,
    callback=delivery_report
)

producer.flush()