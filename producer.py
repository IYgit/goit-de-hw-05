from kafka import KafkaProducer
from configs import kafka_config
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'Ivan_building_sensors'

for i in range(100):  # Generate data indefinitely
    data = {
        "sensor_id": random.randint(1, 100),
        "timestamp": int(time.time()),
        "temperature": random.uniform(25, 45),
        "humidity": random.uniform(15, 85)
    }
    producer.send(topic_name, value=data)
    producer.flush()
    print(f"Data sent: {data}")
    time.sleep(1)

producer.close()
