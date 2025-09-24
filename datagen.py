from pyflink.table import EnvironmentSettings, TableEnvironment
import pandas as pd
from faker import Faker
import random
import time

fake = Faker()
records = 1000
maxTransaction = 10000

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

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

conf = {
    'bootstrap.servers': 'localhost:9092',  
    'client.id': 'transaction-producer'
}

def generate_data():
    for _ in range(records):
        yield {
            'TransactionID': fake.uuid4(),
            'CardID': fake.credit_card_number(),
            'Merchant': random.choice(merchants),
            'Amount': round(random.uniform(0, maxTransaction), 2),
            'TimestampT': str(fake.date_time_between(start_date='-1d', end_date='now'))
        }

while True:
    df = pd.DataFrame(generate_data())
    print(df.head(5))
    df.to_csv("/tmp/fake_data.csv", index=False)

    table_env.execute_sql("DROP TABLE IF EXISTS source")
    table_env.execute_sql(f"""
    CREATE TABLE source (
        TransactionID INT,
        CardID INT,
        Merchant STRING,
        amount DOUBLE,
        TimestampT TIMESTAMP(3)
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///tmp/fake_data.csv',
        'format' = 'csv'
    )
""")

    table_env.execute_sql("DROP TABLE IF EXISTS sink")
    table_env.execute_sql(f"""
    CREATE TABLE source (
        TransactionID INT,
        CardID INT,
        Merchant STRING,
        amount DOUBLE,
        TimestampT TIMESTAMP(3)
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///tmp/fake_data.csv',
        'format' = 'csv'
    )
""")

    table_env.execute_sql("""
    CREATE TABLE sink (
        TransactionID INT,
        CardID INT,
        Merchant STRING,
        amount DOUBLE,
        TimestampT TIMESTAMP(3)
    ) WITH (
        'connector' = 'print'
    )
""")
    table_env.execute_sql("""
    INSERT INTO sink
    SELECT TransactionID, CardID, Merchant, amount,TimestampT
    FROM source
    WHERE amount > 0
""")

    time.sleep(10)
