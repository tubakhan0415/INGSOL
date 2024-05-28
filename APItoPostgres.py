import requests
import psycopg2
import json
import time
from psycopg2.extras import DateTimeTZRange
from datetime import datetime

API_ENDPOINT = 'http://192.168.2.242:80/api/v1'
HEADERS = {'accept-encoding': 'false', 'Connection': 'keep-alive'}
DB_HOST = 'localhost'
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'Tub@0415'
DB_PORT = '5433'

# Correctly format the URL
url = f'{API_ENDPOINT}/measurements'

# Create the SQL table
def create_table():
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                                user=DB_USER, password=DB_PASSWORD)
        cursor = conn.cursor()
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS Measurements (
                timestamp TIMESTAMP,
                name VARCHAR(102),
                value FLOAT,
                unit VARCHAR(109),
                description VARCHAR
            )
        '''
        cursor.execute(create_table_query)
        conn.commit()
        conn.close()
        print("Table 'Measurements' created successfully.")
    except Exception as e:
        print("Error while creating table in PostgreSQL:", e)

# Fetch data from the REST API
def fetch_data_from_rest_api(api_url):
    try:
        res = requests.get(api_url, headers=HEADERS, timeout=10)
        res.raise_for_status()  # Check for any errors in the response
        data = res.json()
        return data
    except requests.exceptions.RequestException as e:
        print("Error while fetching data from the REST API:", e)
        return None
    except json.JSONDecodeError as e:
        print("Error while parsing JSON data from the REST API response:", e)
        return None

# Insert data into PostgreSQL
def insert_data_into_postgres(data):
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                                user=DB_USER, password=DB_PASSWORD)
        cursor = conn.cursor()

     # Convert the timestamp string to a valid timestamp format
        timestamp = datetime.fromisoformat(data['timestamp'].rstrip('Z'))

        for item in data['items']:
            cursor.execute("INSERT INTO Measurements (timestamp, name, value, unit, description) VALUES ( %s, %s, %s, %s, %s)",
                       ( timestamp, item['name'], item['value'], item['unit'], item['description']))
        conn.commit()
        conn.close()
        print("Data inserted into PostgreSQL successfully.")


    except psycopg2.Error as e:
        print("Error while inserting data into PostgreSQL:", e)
        
    except Exception as e:
        print("Error while inserting data into PostgreSQL:", e)

# Main function
def main():
   while True:
        data = fetch_data_from_rest_api(url)
        if data is not None:
            insert_data_into_postgres(data)
        time.sleep(0.001)
        
if __name__ == "__main__":
    create_table()
    main()
