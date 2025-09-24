df = pd.DataFrame(generate_data())
df.to_csv("/tmp/fake_data.csv", index=False, header=False)

table_env.execute_sql("DROP TABLE IF EXISTS source")
table_env.execute_sql("DROP TABLE IF EXISTS sink")

table_env.execute_sql("""
    CREATE TABLE source (
        TransactionID STRING,
        CardID STRING,
        Merchant STRING,
        Amount FLOAT,
        TimestampT TIMESTAMP(3)
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///tmp/fake_data.csv',
        'format' = 'csv'
    )
""")

table_env.execute_sql("""
    CREATE TABLE sink (
        TransactionID STRING,
        CardID STRING,
        Merchant STRING,
        Amount DOUBLE,
        TimestampT TIMESTAMP(3)
    ) WITH (
        'connector' = 'print'
    )
""")


table_env.execute_sql("""
    INSERT INTO sink
    SELECT TransactionID, CardID, Merchant, Amount, TimestampT
    FROM source
    WHERE Amount > 0
""").wait()  
