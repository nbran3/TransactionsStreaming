from faker import Faker
import pandas as pd
import random
import time

fake = Faker()
records = 1000
maxTransaction = 10000

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

while True:
    data = {
    'TransactionID': [fake.uuid4() for _ in range(records)],
    'CardID': [fake.credit_card_number() for _ in range(records)],
    'Merchant': [fake.random_element(elements=merchants) for _ in range(records)],
    'Amount': [round(random.uniform(0, maxTransaction), 2) for _ in range(records)],
    'Timestamp': [fake.date_time_between(start_date='-1d', end_date='now') for _ in range(records)]
    }

    df = pd.DataFrame(data)

    print(df.head())
    time.sleep(10)

    