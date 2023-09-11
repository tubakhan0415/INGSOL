import requests
import time
import paho.mqtt.client as paho
from paho import mqtt
import json


API_ENDPOINT = 'http://192.168.2.242:80/api/v1'
HEADERS = {'accept-encoding': 'false', 'Connection': 'keep-alive'}

# Set the interval in seconds between API requests
interval_seconds = 0.001  # For example, fetch data every 60 seconds

# Correctly format the URL
url = f'{API_ENDPOINT}/measurements'

# MQTT configuration
mqtt_broker_host = "e82fd245370b45908648b0e95245cb0e.s1.eu.hivemq.cloud"
mqtt_broker_port = 8883
mqtt_topic = "empro"

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

# setting callbacks for different events to see if it works, print the message etc.
def on_connect(client, userdata, flags, rc, properties=None):
    print("CONNACK received with code %s." % rc)

# with this callback you can see if your publish was successful
def on_publish(client, userdata, mid, properties=None):
    print("mid: " + str(mid))



# print message, useful for checking if it was successful
def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

# using MQTT version 5 here, for 3.1.1: MQTTv311, 3.1: MQTTv31
# userdata is user defined data of any type, updated by user_data_set()
# client_id is the given name of the client
client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
client.on_connect = on_connect
client.on_publish = on_publish

# enable TLS for secure connection
client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
# set username and password
client.username_pw_set("empro", "Tub@0415")
# connect to HiveMQ Cloud on port 8883 (default for MQTT)
client.connect(mqtt_broker_host,mqtt_broker_port)


# Start the MQTT loop in a separate thread
client.loop_start()

try:
    while True:
        response = requests.get(url, headers=HEADERS)

        if response.status_code == 200:
            data = response.json()
            print(data)
            # Publish the fetched data to the MQTT topic
            client.publish(mqtt_topic, json.dumps(data), qos=0)
            print("Data published to MQTT topic.")
        else:
            print("Error:", response.status_code)

        time.sleep(interval_seconds)

except KeyboardInterrupt:
    print("Interrupted. Stopping the MQTT loop...")
    client.loop_stop()





# loop_forever for simplicity, here you need to stop the loop manually
# you can also use loop_start and loop_stop
client.loop_forever()
