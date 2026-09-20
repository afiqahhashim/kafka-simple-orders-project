# kafka-simple-orders-project
This project showcases a very basic usage of kafka and docker. I also use database postgreSQL as a place to store the order data.


1. database.py:
   This file is used to create database 'kafkadb_01' and table 'orders'.
   Table 'orders' contain 4 columns: id, user_name, item, quantity.

2. docker-compose.yml:
   This file is used to define the configuration of a multi-container application, including the services (e.g., Kafka, PostgreSQL) used in    the containers.

3. producer.py:
   The producer.py file is for sending the message to Kafka

4. tracker.py:
   This file act as a consumer. It read messages that send from producer.py and insert into database.
