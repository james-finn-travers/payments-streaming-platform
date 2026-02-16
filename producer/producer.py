"""
Synthetic financial transaction data generator using Faker library.
"""
import json
import uuid
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer


def generate_transaction() -> dict:
    """
    Generate a synthetic financial transaction with realistic data.

    Returns:
        dict: Transaction data containing:
            - transaction_id (UUID): Unique transaction identifier
            - user_id (str): User identifier
            - amount (float): Transaction amount
            - currency (str): Currency code (CAD)
            - timestamp (datetime): Transaction timestamp
            - merchant_id (str): Merchant identifier
            - location (dict): Geographic coordinates with lat/lon
    """
    fake = Faker()

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": fake.uuid4(),
        "amount": round(fake.random.uniform(1.0, 10000.0), 2),
        "currency": "CAD",
        "timestamp": fake.date_time_this_year().isoformat(),
        "merchant_id": fake.uuid4(),
        "location": {
            "lat": float(fake.latitude()),
            "lon": float(fake.longitude())
        }
    }

    return transaction


if __name__ == "__main__":
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Example usage
    transaction = generate_transaction()
    producer.send('transactions', transaction)
    producer.flush()
    print("Transaction sent to Kafka:", transaction)
