import RPi.GPIO as GPIO
import streamlit as st
from PIL import Image
import io
import cv2
from google.cloud import vision
from google.oauth2 import service_account
import psycopg2
import time
import atexit

# Set up Raspberry Pi GPIO
sensor_pin = 23
GPIO.setmode(GPIO.BCM)
GPIO.setup(sensor_pin, GPIO.IN)  # Replace 23 with the actual GPIO pin you're using

# Specify the path to your credentials JSON file
credentials_path = '/home/sars/Downloads/noted-aloe.json'

# Create a Vision API client with the credentials file
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client_vision = vision.ImageAnnotatorClient(credentials=credentials)

# PostgreSQL database settings
db_host = 'localhost'
db_port = '5432'
db_name = 'postgres'
db_user = 'postgres'
db_password = 'Tub@0415'

# Global variables
auto_trigger = False
connection = None
user_name = None  # Define user_name as a global variable

# Function to detect GPIO pin state
def detect_gpio_state():
    # Read the state of GPIO 23
    sensor_state = GPIO.input(sensor_pin)
    print(sensor_state)

    # Check if the sensor is high or low
    if sensor_state == GPIO.HIGH:
        print("Sensor is HIGH")
        return True
    else:
        print("Sensor is LOW")
        return False

# Function to connect to PostgreSQL
def connect_to_postgresql():
    try:
        return psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
    except psycopg2.Error as e:
        st.error(f"Error connecting to PostgreSQL: {e}")
        return None

# Function to extract text from an image using Google Cloud Vision API
def extract_text_from_image(image):
    image_pil = Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
    response = client_vision.text_detection(image=vision.Image(content=cv2.imencode(".jpg", cv2.cvtColor(image, cv2.COLOR_BGR2RGB))[1].tobytes()))
    texts = response.text_annotations

    if texts:
        return texts[0].description
    else:
        return "No text found"

# Function to create a table if not exists
def create_table_if_not_exists(connection, store_info):
    cursor = None

    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {store_info} (id SERIAL PRIMARY KEY, text_column VARCHAR, column1_data VARCHAR, column2_data VARCHAR, column3_data VARCHAR, column4_data VARCHAR);")
        connection.commit()

    except Exception as e:
        st.error(f"Error creating table {store_info}: {e}")

    finally:
        if cursor:
            cursor.close()

# Function to insert data into PostgreSQL with additional columns
def insert_data_into_postgresql(text, column1_data, column2_data, column3_data, column4_data, store_info, connection):
    cursor = None

    try:
        cursor = connection.cursor()
        create_table_if_not_exists(connection, store_info)

        query = f"INSERT INTO {store_info} (text_column, column1_data, column2_data, column3_data, column4_data) VALUES (%s, %s, %s, %s, %s) RETURNING id;"
        data = (text, column1_data, column2_data, column3_data, column4_data)

        print("Executing query:", query)
        print("Data:", data)

        cursor.execute(query, data)
        connection.commit()

        generated_id = cursor.fetchone()[0]
        st.text(f"{store_info.capitalize()} - Generated ID: {generated_id}")

    except psycopg2.Error as pe:
        st.error(f"Error inserting data into PostgreSQL: {pe}")
        st.error(f"Query: {query}")
        st.error(f"Data: {data}")

    finally:
        if cursor:
            cursor.close()

# Function to compare extracted text with stored data
def compare_text_with_stored_info(text, store_info, connection):
    cursor = None

    try:
        cursor = connection.cursor()
        create_table_if_not_exists(connection, store_info)

        cursor.execute(f"SELECT text_column, column1_data, column2_data, column3_data, column4_data FROM {store_info};")
        stored_data = cursor.fetchall()

        matches = []
        for row in stored_data:
            row_words = [word for cell in row for word in cell.split()]
            if any(word in text for word in row_words):
                matches.extend([word for word in row_words if word in text])

        return matches

    except Exception as e:
        st.error(f"Error comparing text with stored info: {e}")
        return []

    finally:
        if cursor:
            cursor.close()

# Function to store the captured image in the captured_data table
def store_captured_image(image, connection):
    cursor = None

    try:
        cursor = connection.cursor()
        
        cursor.execute(f"CREATE TABLE IF NOT EXISTS captured_text (id SERIAL PRIMARY KEY, user_name VARCHAR, image_column BYTEA);")

        image_bytes = cv2.imencode(".jpg", image)[1].tobytes()
        
        cursor.execute(f"INSERT INTO captured_text (user_name, image_column) VALUES (%s, %s) RETURNING id;", (user_name, image_bytes))

        connection.commit()

        result = cursor.fetchone()
        generated_id = result[0] if result else None

        st.text(f"captured_text - Generated ID: {generated_id}")

    except Exception as e:
        st.error(f"Error inserting data into PostgreSQL: {e}")

    finally:
        if cursor:
            cursor.close()

# Define GPIO pins for LEDs
green_led_pin = 17  # GPIO pin for green LED
red_led_pin = 25    # GPIO pin for red LED

# Function to control LED based on comparison result
def control_led(led_pin, state, delay):
    GPIO.setup(led_pin, GPIO.OUT)
    GPIO.output(led_pin, state)

# Function to control LED based on comparison result with delay
def control_led_with_delay(led_pin, state):
    GPIO.setup(led_pin, GPIO.OUT)
    GPIO.output(led_pin, state)

    # Display LED status immediately on the web app
    led_status = "Green" if led_pin == green_led_pin else "Red"
    st.markdown(f"**{led_status} LED is ON**")

    time.sleep(delay)/home/sars/Downloads
    GPIO.output(led_pin, GPIO.LOW)  # Turn off the LED after the delay

def capture_extract_compare_page():
    global auto_trigger, connection, user_name  # Declare variables as global

    st.title("Capture, Extract, and Compare Page")

    while not GPIO.input(sensor_pin):
        pass

    # Wait for the GPIO pin to be in a high state
    st.text("Waiting for GPIO pin to be HIGH...")
    while not detect_gpio_state():
        time.sleep(1)  # Add /home/sars/Downloadsa short delay to avoid excessive CPU usage

    st.text("GPIO pin is HIGH. Starting the process...")

    try:
        # OpenCV VideoCapture to capture frames from the camera
        cap = cv2.VideoCapture(0)

        if not cap.isOpened():
            raise RuntimeError("Error: Could not open camera.")

        st.text(f"Camera opened successfully: {cap.isOpened()}")

        while GPIO.input(sensor_pin):  # Read the frame
            ret, frame = cap.read()

            # Check if the frame is valid
            if not ret:
                print("Error: Could not read frame.")
                break

            # Extract text for every frame
            text = extract_text_from_image(frame)
            print("Extracted Text:", text)

            # Compare extracted text with stored data in the store_info table
            matches = compare_text_with_stored_info(text, "store_info", connection)

            # Display the result of the comparison
            st.markdown(f"**Comparison Result with store_info: {'Yes' if matches else 'No'}**")

            # Display the matched text
            if matches:
                st.markdown(f"**Matched Text: {', '.join(matches)}**")
                # Turn on the green LED with a delay of 10 seconds
                control_led_with_delay(green_led_pin, GPIO.HIGH, 7)
            else:
                # Turn on the red LED with a delay of 10 seconds
                control_led_with_delay(red_led_pin, GPIO.HIGH, 7)

            # Introduce a delay after turning off the LED and before updating the status on the web app
            time.sleep(2)  # You can adjust the delay time as needed

            # Turn off the LEDs
            control_led(green_led_pin, GPIO.LOW)
            control_led(red_led_pin, GPIO.LOW)

            # Display LED off status on the web app
            leds_off_status = ", ".join([f"{color} LED" for color in ["Green", "Red"]])
            st.markdown(f"**{leds_off_status} are OFF**")

            # If there are matches, store the captured image in the captured_data table
            if matches:
                store_captured_image(image=frame, connection=connection)

            # Reset the flag to avoid repeated extraction
            auto_trigger = False

    except Exception as e:
        st.error(f"Error in capture_extract_compare_page: {e}")

    finally:
        # Release the camera
        if 'cap' in locals() and cap.isOpened():
            cap.release()

        # Clean up GPIO
        if GPIO.getmode() is not None:
            GPIO.cleanup()

# Function for the Authentication Page
def authentication_page():
    global user_name

    st.title("Authentication Page")

    user_name = st.text_input("Enter Your Name:")
    if st.button("Authenticate"):
        st.success(f"Hello, {user_name}! Authentication successful.")

# Function for the Store Data Page
def store_info_page():
    global connection

    st.title("Store Information Page")

    text_column = st.text_input("Enter UserName:")
    column1_data = st.text_input("Enter Manufacturer Name:")
    column2_data = st.text_input("Enter Brand Name:")
    column3_data = st.text_input("Enter Family Name:")
    column4_data = st.text_input("Enter Polymer Name:")

    if st.button("Store Data"):
        connection = connect_to_postgresql()

        if connection:
            insert_data_into_postgresql(text_column, column1_data, column2_data, column3_data, column4_data, "store_info", connection)

# Streamlit app
def main():
    global auto_trigger, connection, user_name

    page = st.sidebar.selectbox("Select a page", ["Authentication", "Store Information", "Capture, Extract, and Compare"])

    if page == "Authentication":
        authentication_page()
    elif page == "Store Information":
        store_info_page()
    elif page == "Capture, Extract, and Compare":
        connection = connect_to_postgresql()
        if connection:
            capture_extract_compare_page()

if __name__ == "__main__":
    main()
