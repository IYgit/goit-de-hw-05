from kafka import KafkaConsumer
import json

consumer_temp = KafkaConsumer(
    'Ivan_temperature_alerts',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

consumer_humidity = KafkaConsumer(
    'Ivan_humidity_alerts',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer_temp:
    print("Temperature Alert:", message.value)

for message in consumer_humidity:
    print("Humidity Alert:", message.value)
