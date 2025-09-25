from confluent_kafka import Producer
from dotenv import load_dotenv
import json, random, time
from faker import Faker
import os

load_dotenv()

fake = Faker()

api_key = os.environ.get("CONFLUENT_API_KEY")
api_secret = os.environ.get("CONFLUENT_API_SECRET")
cluster_url = os.environ.get("CLUSTER_URL")

conf = {
    'bootstrap.servers': cluster_url,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': api_key,
    'sasl.password': api_secret,
}
producer = Producer(conf)

fake = Faker()
records = 1000
maxTransaction = 10000
counter = 0


merchants = [
    'Amazon', 'Walmart', 'Target', 'Costco', 'Best Buy', 'Home Depot', 'Lowe\'s',
    'Macy\'s', 'Kohl\'s', 'Nordstrom', 'eBay',
    'McDonald\'s', 'Starbucks', 'Subway', 'Chick-fil-A', 'Chipotle', 'Dunkin\'',
    'Taco Bell', 'Domino\'s Pizza', 'Panera Bread', 'Shake Shack',
    'Shell', 'Chevron', 'ExxonMobil', 'BP', '7-Eleven', 'Speedway',
    'Delta Airlines', 'United Airlines', 'American Airlines',
    'Marriott Hotels', 'Hilton Hotels', 'Hyatt',
    'Uber', 'Lyft', 'Airbnb',
    'Netflix', 'Spotify', 'Apple', 'Google Play',
    'Disney+', 'Hulu', 'AMC Theatres',
    'CVS Pharmacy', 'Walgreens', 'Rite Aid', 'GNC', 'Planet Fitness',
]

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_data():
    for _ in range(records):
        yield {
            'TransactionID': fake.uuid4(), 
            'CardID': fake.credit_card_number(),  
            'Merchant': random.choice(merchants),
            'Amount': round(random.uniform(0, maxTransaction), 2),
            'TimestampT': fake.date_time_between(
                start_date='-1d', end_date='now'
            ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] 
        }

try:
    print("Starting producer. Press Ctrl+C to stop.")
    while True:
        counter += 1
        for data in generate_data():
            producer.produce(
                "transcations",
                key=str(data["TransactionID"]),
                value=json.dumps(data)
            )
            producer.poll(0)  
        print(f"Batch {counter} sent, sleeping...")
        time.sleep(2)  
except KeyboardInterrupt:
    print("\n Stopping producer...")
finally:

    producer.flush()
    print("All messages flushed. Exiting.")

