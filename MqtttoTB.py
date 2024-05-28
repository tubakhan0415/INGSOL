import json
import time
import psycopg2
from datetime import datetime
import paho.mqtt.client as paho

# PostgreSQL settings
DB_HOST = 'mpsrejcxc7.ngjos0yk65.tsdb.cloud.timescale.com'
DB_PORT = 38875
DB_NAME = 'tsdb'
DB_USER = 'tsdbadmin'
DB_PASSWORD = 'Tub@khan0415'

# Create the SQL tables
def create_tables():
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                                user=DB_USER, password=DB_PASSWORD)
        cursor = conn.cursor()
        
        # Create CommonData table
        create_common_data_table_query = '''
            CREATE TABLE IF NOT EXISTS CommonData (
                id SERIAL PRIMARY KEY,
                name VARCHAR(102),
                description VARCHAR,
                unit VARCHAR(109)
            )
        '''
        cursor.execute(create_common_data_table_query)
        
        # Create OtherData table with foreign key reference to CommonData
        create_other_data_table_query = '''
            CREATE TABLE IF NOT EXISTS OtherData (
                timestamp TIMESTAMP,
                value FLOAT,
                common_data_id INT REFERENCES CommonData(id),
                tk TIMESTAMP DEFAULT NOW()
            )
        '''
        cursor.execute(create_other_data_table_query)
        
        conn.commit()
        conn.close()
        print("Tables 'CommonData' and 'OtherData' created successfully.")
    except Exception as e:
        print("Error while creating tables in PostgreSQL:", e)

# Insert data into PostgreSQL
def insert_data_into_postgres(item, timestamp):
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                                user=DB_USER, password=DB_PASSWORD)
        cursor = conn.cursor()

        # Check if common data (name, description, unit) already exists
        cursor.execute("SELECT id FROM CommonData WHERE name = %s AND description = %s AND unit = %s",
                       (item['name'], item['description'], item['unit']))
        common_data = cursor.fetchone()
        
        if not common_data:
            # If common data doesn't exist, insert into CommonData table
            cursor.execute("INSERT INTO CommonData (name, description, unit) VALUES (%s, %s, %s) RETURNING id",
                           (item['name'], item['description'], item['unit']))
            common_data_id = cursor.fetchone()[0]
        else:
            common_data_id = common_data[0]
        
        # Insert data into OtherData table with foreign key reference to CommonData
        cursor.execute("INSERT INTO OtherData (timestamp, value, common_data_id) VALUES (%s, %s, %s)",
                       (timestamp, item['value'], common_data_id))
        
        conn.commit()
        conn.close()
        print("Data inserted into PostgreSQL successfully.")

    except psycopg2.Error as e:
        print("Error while inserting data into PostgreSQL:", e)

    except Exception as e:
        print("Error while inserting data into PostgreSQL:", e)

# MQTT callbacks
def on_connect(client, userdata, flags, rc, properties=None):
    print("CONNACK received with code %s." % rc)

def on_publish(client, userdata, mid):
    print("mid: " + str(mid))

def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

def on_message(client, userdata, msg):
    try:
        print("Received a message from MQTT topic:", msg.topic)
        print("Message payload:", msg.payload)

        if msg.payload:
            print("Attempting to parse JSON...")
            data = json.loads(msg.payload)
            print("Parsed data:", data)

            # Extract relevant fields
            timestamp = data['timestamp']
            items = data['items']

            # Insert data into PostgreSQL
            for item in items:
                insert_data_into_postgres(item, timestamp)

    except json.JSONDecodeError as e:
        print("Error while parsing JSON data:", e)

    except Exception as e:
        print("Error:", e)

# Main
create_tables()

client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
client.on_connect = on_connect
client.tls_set(tls_version=paho.ssl.PROTOCOL_TLS)
client.username_pw_set("empro", "Tub@0415")
client.connect("e82fd245370b45908648b0e95245cb0e.s1.eu.hivemq.cloud", 8883)
client.on_subscribe = on_subscribe
client.on_message = on_message
client.on_publish = on_publish

client.subscribe("empro", qos=0)

client.loop_forever()
