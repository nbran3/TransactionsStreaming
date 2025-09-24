from confluent_kafka import Producer
import json, random, time
from faker import Faker
import os

fake = Faker()

api_key = os.environ.get("CONFLUENT_API_KEY")
api_secret = os.environ.get("CONFLUENT_API_SECRET")
cluster_url = os.environ.get("YOUR_CLUSTER_URL")
sasl_ssl_key = os.environ.get("SASL_SSL")

conf = {
    'bootstrap.servers': cluster_url,
    'security.protocol': sasl_ssl_key,
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

while True:
    counter += 1
    data = generate_data()
    producer.produce("transactions", key=str(data["card_id"]), value=json.dumps(data))
    producer.flush()
    print(f"Sent: {counter} batch(es)")
    time.sleep(1)

