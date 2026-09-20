from confluent_kafka import Consumer
import json
import psycopg2

# connect to database connection
db_connection = psycopg2.connect(
    host='localhost',
    port = 5432,
    user='postgres',
    database = 'kafka01_db',
    password='1234'
)

cursor = db_connection.cursor()

#  connect to kafka broker
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-tracker', #identifies group of consumers
    'auto.offset.reset': 'earliest' # auto reset to the earliest offset
}

consumer = Consumer(consumer_config)

consumer.subscribe(['orders']) # 1 consumer can subs to multiple topics

print('Consumer is running and subscribed to orders topic')



# logic to check if there are any new events in the subscribed topic

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            print('Consumer error: ', msg.error())
            continue

        # change Kafka bytes -> JSON string
        value = msg.value().decode('utf-8')

        # change JSON string -> Python dict
        order = json.loads(value)

        print(f'Received order: {order['item']} x {order['quantity']} from {order["user_name"]}')


        # Save order into database: kafka01_db.orders

        insert_query = ''' INSERT INTO orders (
                        id,
                        user_name,
                        item,
                        quantity 
                        ) 
                        VALUES (%s, %s, %s, %s)'''

        cursor.execute(insert_query, (order['id'], order['user_name'], order['item'], order['quantity']))

        db_connection.commit()

        print('Order saved to database.')

except KeyboardInterrupt:
    print('\n Stopping consumer')

finally:
    consumer.close()
    cursor.close()
    db_connection.close()