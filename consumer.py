# from kafka import KafkaConsumer, KafkaProducer
# from configs import kafka_config
# import json

# consumer = KafkaConsumer(
#     'Ivan_building_sensors',
#     bootstrap_servers=kafka_config['bootstrap_servers'],
#     security_protocol=kafka_config['security_protocol'],
#     sasl_mechanism=kafka_config['sasl_mechanism'],
#     sasl_plain_username=kafka_config['username'],
#     sasl_plain_password=kafka_config['password'],
#     value_deserializer=lambda v: json.loads(v.decode('utf-8')),
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     group_id='sensor_alerts_processor'
# )

# producer = KafkaProducer(
#     bootstrap_servers=kafka_config['bootstrap_servers'],
#     security_protocol=kafka_config['security_protocol'],
#     sasl_mechanism=kafka_config['sasl_mechanism'],
#     sasl_plain_username=kafka_config['username'],
#     sasl_plain_password=kafka_config['password'],
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# for message in consumer:
#     data = message.value
#     if data['temperature'] > 40:
#         producer.send('Ivan_temperature_alerts', value={"sensor_id": data['sensor_id'], "alert": "High temperature"})
#     if data['humidity'] > 80 or data['humidity'] < 20:
#         producer.send('Ivan_humidity_alerts', value={"sensor_id": data['sensor_id'], "alert": "Abnormal humidity"})
#     producer.flush()

# consumer.close()
# producer.close()


from kafka import KafkaConsumer, KafkaProducer
import json
from configs import kafka_config

consumer = KafkaConsumer(
    'Ivan_building_sensors',
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='sensor_alerts_processor'
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for message in consumer:
    data = message.value
    print("Received data:", data)  # Вивід отриманих даних

    if data['temperature'] > 40:
        alert_temp = {"sensor_id": data['sensor_id'], "alert": "High temperature"}
        producer.send('Ivan_temperature_alerts', value=alert_temp)
        print("Sent to Ivan_temperature_alerts:", alert_temp)  # Вивід даних, що відправляються по температурі

    if data['humidity'] > 80 or data['humidity'] < 20:
        alert_humidity = {"sensor_id": data['sensor_id'], "alert": "Abnormal humidity"}
        producer.send('Ivan_humidity_alerts', value=alert_humidity)
        print("Sent to Ivan_humidity_alerts:", alert_humidity)  # Вивід даних, що відправляються по вологості

    producer.flush()

consumer.close()
producer.close()
