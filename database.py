# This file's purpose is to create the database

import psycopg2 as pgsql

#connect to postgreSQL server
connection = pgsql.connect(
    host='localhost',
    port = 5432,
    user='postgres',
    database = 'kafka01_db',
    password='1234')

connection.autocommit = True
cursor = connection.cursor()


#create database
#cursor.execute('''CREATE DATABASE kafka01_db ''')


#create table orders
cursor.execute('''CREATE TABLE IF NOT EXISTS orders ( 
                id varchar(100) primary key,
                user_name varchar(100),
                item varchar(100),
                quantity int)
''')

connection.commit()
print("Table 'orders' in 'kafka01_db' database successfully created")

#close connection
cursor.close()
connection.close()