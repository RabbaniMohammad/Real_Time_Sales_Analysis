# Combines the topic and push it to the topic called processed_sales_info

from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
import pandas as pd
import json

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'raw_sales_info',
    'auto.offset.reset': 'earliest',  # Start consuming from the earliest offset if no committed offset is found
    'enable.auto.commit': False       # Disable auto-commit to ensure manual offset management
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['sales_info'])  # Topic to consume

# Kafka Producer Configuration
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
}
producer = SerializingProducer(producer_conf)

# Initialize an empty DataFrame
# columns = [
#     "customer_id", "product_id", "product_name", "quantity", "state", "city", "branch",
#     "timestamp", "date", "time", "month", "year", "shopping_experience", "payment_method", "total_amount"
# ]

columns = [
    "customer_id", "product_name", "quantity", "state", "city", "branch",
    "timestamp", "date", "time", "month", "year", "shopping_experience", "payment_method", "total_amount"
]
sales_df = pd.DataFrame(columns=columns)

# Main Consumer Loop
try:
    while True:
        # Poll for messages
        msg = consumer.poll(timeout=0.1)

        if msg is None:
            continue
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break
        else:
            # Deserialize the message
            sales = json.loads(msg.value().decode('utf-8'))
            # print(f"Consumed Message: {sales}")

            # Append the message to the DataFrame
            new_row = pd.DataFrame([sales])
            sales_df = pd.concat([sales_df, new_row], ignore_index=True)

            print("This is the updated dataframe", sales_df)

            # Push the processed record to a new topic
            sales_dict = sales_df.to_dict(orient='records')

            print("This is a updated data with the dict", sales_dict)
            producer.produce(
                topic='processed_sales_info',  # Target topic
                key=str(sales['customer_id']),  # Key for partitioning
                value=json.dumps(sales_dict)        # Serialized JSON value
            )
            producer.poll(0)  # Ensure delivery
            # print(f"Pushed to processed_sales_info: {sales}")

except KeyboardInterrupt:
    print("Consumer interrupted.")
except KafkaException as e:
    print(f"Kafka exception occurred: {e}")
finally:
    consumer.close()
    print("Consumer closed.")
