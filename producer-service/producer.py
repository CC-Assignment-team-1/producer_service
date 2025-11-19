import json
import random
import time
import os
from confluent_kafka import Producer

# Configuration from environment variables
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
api_key = os.getenv('KAFKA_API_KEY')
api_secret = os.getenv('KAFKA_API_SECRET')

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': api_key,
    'sasl.password': api_secret,
    'client.id': 'random-producer'
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_random_number():
    while True:
        random_number = random.randint(0, 1000)
        message = {
            'value': random_number,
            'timestamp': time.time()
        }
        
        producer.produce(
            'topic1',
            key=str(random_number).encode('utf-8'),
            value=json.dumps(message).encode('utf-8'),
            callback=delivery_report
        )
        
        producer.poll(0)
        print(f'Produced: {random_number}')
        time.sleep(1)

if __name__ == '__main__':
    try:
        produce_random_number()
    except KeyboardInterrupt:
        print('Producer stopped')
    finally:
        producer.flush()
