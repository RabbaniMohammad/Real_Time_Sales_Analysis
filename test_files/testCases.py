from confluent_kafka import Producer, Consumer
import json
import unittest

class TestKafkaIntegration(unittest.TestCase):
    producer_conf = {
        'bootstrap.servers': 'localhost:9092',
    }

    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test_group',
        'auto.offset.reset': 'earliest',
    }

    test_topic = 'test_group'
    processed_topic = 'test_group'

    def setUp(self):
        self.producer = Producer(self.producer_conf)
        self.consumer = Consumer(self.consumer_conf)
        self.consumer.subscribe([self.processed_topic])

    def tearDown(self):
        self.consumer.close()

    def test_message_processing(self):
        # Produce a test message
        test_message = {
            "customer_id": 1,
            "product_id": 1,
            "product_name": "Wireless Mouse",
            "quantity": 1,
            "state": "Connecticut",
            "city": "Hartford",
            "branch": "Downtown",
            "timestamp": "2024-12-21T12:00:00",
            "date": "2024-12-21",
            "time": "12:00:00",
            "month": 12,
            "year": 2024,
            "shopping_experience": "It was excellent!",
            "payment_method": "Credit Card",
            "total_amount": 25.00
        }
        self.producer.produce(self.test_topic, value=json.dumps(test_message))
        self.producer.flush()


        # Consume the message from the processed topic
        msg = self.consumer.poll(timeout=10.0)

        self.assertIsNotNone(msg)
        processed_data = json.loads(msg.value().decode('utf-8'))

        # Validate processed message
        self.assertEqual(processed_data['product_name'], "Wireless Mouse")
        self.assertEqual(processed_data['state'], "Connecticut")

if __name__ == '__main__':
    unittest.main()
