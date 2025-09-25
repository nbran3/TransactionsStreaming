# Real-time Streaming Transactions Data Pipeline


## This project looks to use Apache Kafka to mimic what streaming real-time transaction data into a database for live dashboards would look like.
  
## Tech Stack
- Python
- Google Cloud 
- Apache Kafka (Confluent)
- Looker Studio

This project was mainly a project for me to get to know Apache Kafka. I wanted to do a streaming data pipeline for some time, but a big hurdle was trying to figure out the data. Most of the data that could make a streaming pipeline is usually paywalled, so I decided to use a Python library called "Faker"
to generate synthetic credit card transactions. This library allowed me to create fake Transaction IDs, card numbers, and times. I more or less used the built-in Python random library for the other variables, such as Merchant, Amount, and Category, which, for the most part, were defined by me. There is only one script, the data is generated and then pushed to Kafka via Confluent
(their servers) and then pushed into BigQuery using a sink connector from Confluent. Other than the data generation, most of this project was making sure the connections were in place. The BigQuery table was manually defined by me as well. I used Looker Studio for the dashboard as it is very easy to connect to BigQuery, but I do not have the 
premium plan for Looker, so my dashboard would not update until I refreshed the page. 

Dashboard - https://lookerstudio.google.com/s/hZlJAhsbNpo

Video of pipeline - https://www.youtube.com/watch?v=g1BVaShAr2w
