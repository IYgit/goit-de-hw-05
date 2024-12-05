from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

def create_topic(name, partitions, replication):
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']
    )

    topic = NewTopic(name=name, num_partitions=partitions, replication_factor=replication)
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{name}' created successfully.")
    except Exception as e:
        print(f"An error occurred while creating topic {name}: {e}")
    finally:
        admin_client.close()

create_topic('Ivan_building_sensors', 3, 1)
create_topic('Ivan_temperature_alerts', 1, 1)
create_topic('Ivan_humidity_alerts', 1, 1)
