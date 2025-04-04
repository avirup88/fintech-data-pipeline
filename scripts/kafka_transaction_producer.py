import json
import time
import random
import uuid
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

fake = Faker()

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "transactions_stream"

# Test Kafka connection before starting producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        retries=5,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("Connected to Kafka successfully.")
except KafkaError as e:
    print(f"Failed to connect to Kafka: {e}")
    exit(1)

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": fake.random_int(min=1000, max=9999),
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "timestamp": fake.iso8601(),
        "merchant": fake.company(),
        "location": fake.city(),
        "payment_method": random.choice(["Credit Card", "Debit Card", "PayPal", "Crypto"]),
        "status": random.choice(["Approved", "Declined", "Pending"])
    }

def produce_transactions():
    print(f"Sending transactions to Kafka topic: {KAFKA_TOPIC}")
    while True:
        transaction = generate_transaction()
        try:
            future = producer.send(KAFKA_TOPIC, transaction)
            result = future.get(timeout=5)  # Wait for confirmation
            print(f"Sent: {transaction} | Kafka Ack: {result}")
        except KafkaError as e:
            print(f"Failed to send message: {e}")
        time.sleep(random.uniform(0.5, 2))

if __name__ == "__main__":
    produce_transactions()

