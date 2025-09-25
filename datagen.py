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

records = 1000
maxTransaction = 1000
counter = 0

merchant_categories = {
    'Retail': ['Amazon', 'Walmart', 'Target', 'Costco', 'Best Buy', 'Home Depot', "Lowe's", 'Macy\'s', 'Kohl\'s', 'Nordstrom', 'eBay'],
    'Food & Beverage': ['McDonald\'s', 'Starbucks', 'Subway', 'Chick-fil-A', 'Chipotle', 'Dunkin\'', 'Taco Bell', 'Domino\'s Pizza', 'Panera Bread', 'Shake Shack'],
    'Fuel & Convenience': ['Shell', 'Chevron', 'ExxonMobil', 'BP', '7-Eleven', 'Speedway'],
    'Airlines': ['Delta Airlines', 'United Airlines', 'American Airlines'],
    'Hotels': ['Marriott Hotels', 'Hilton Hotels', 'Hyatt'],
    'Travel & Transport': ['Uber', 'Lyft', 'Airbnb'],
    'Entertainment & Subscriptions': ['Netflix', 'Spotify', 'Apple', 'Google Play', 'Disney+', 'Hulu', 'AMC Theatres'],
    'Pharmacy & Health': ['CVS Pharmacy', 'Walgreens', 'Rite Aid', 'GNC', 'Planet Fitness']
}

amount_ranges = {
    'Retail': (5, 2000),
    'Food & Beverage': (5, 50),
    'Fuel & Convenience': (20, 150),
    'Airlines': (100, 2000),
    'Hotels': (100, 1500),
    'Travel & Transport': (10, 500),
    'Entertainment & Subscriptions': (5, 100),
    'Pharmacy & Health': (5, 300)
}

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def find_category_for_merchant(merchant):
    for category, merchants in merchant_categories.items():
        if merchant in merchants:
            return category
    return None

def generate_data(records=10):
    for _ in range(records):
        category = random.choice(list(merchant_categories.keys()))
        
        merchant = random.choice(merchant_categories[category])
        
        low, high = amount_ranges[category]
        amount = round(random.uniform(low, high), 2)
        
        yield {
            'TransactionID': fake.uuid4(),
            'CardID': fake.credit_card_number(),
            'Merchant': merchant,
            'Category': category,
            'Amount': amount,
            'TimestampT': fake.date_time_between(
                start_date='-1d', end_date='now'
            ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        }

try:
    print("Starting producer. Press Ctrl+C to stop.")
    while True:
        counter += 1
        batch_count = 0
        
        for data in generate_data(records=100):  
            producer.produce(
                "transactions",
                key=str(data["TransactionID"]),
                value=json.dumps(data)
            )
            batch_count += 1
            
        producer.poll(0)
        print(f"Batch {counter} sent ({batch_count} messages), sleeping...")
        time.sleep(2)
        
except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    print("Flushing remaining messages...")
    producer.flush()
    print("All messages flushed. Exiting.")
