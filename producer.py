    # FLOW:
    # 1. Kafka producer
    # 2. orders topic
    # 3. Kafka consumer
    # 4. JSON loads
    # 5. Python dictionary
    # 6. Database INSERT

import json
import uuid

from confluent_kafka import Producer

producer_config = {'bootstrap.servers': 'localhost:9092'}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f'Delivery failed: {err}')
    else:
        print(f'Delivery successful: {msg.value().decode("utf-8")}')
        print(f'Delivered to {msg.topic()}: partition {msg.partition()} offset: {msg.offset()}')

order = {
    'id': str(uuid.uuid4()),
    'user_name': "taylor swift",
    'item': 'cheese pizza',
    'quantity': 4
}

# convert json to json string and encode into byte format for kafka to unedrstand
value = json.dumps(order).encode("utf-8")

producer.produce(
    topic = 'orders',
    value = value,
    callback = delivery_report
)
producer.flush()