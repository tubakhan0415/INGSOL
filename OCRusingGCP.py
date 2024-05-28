import streamlit as st
from PIL import Image
import io
from google.cloud import vision
from google.oauth2 import service_account
import cv2
import numpy as np

# Specify the path to your credentials JSON file
credentials_path = '/home/sars/Downloads/noted-aloe.json'

# Create a Vision API client with the credentials file
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = vision.ImageAnnotatorClient(credentials=credentials)

# Function to extract text using Google Cloud Vision
def extract_text_from_image(image_data):
    if isinstance(image_data, np.ndarray):
        # Data is coming from the camera
        image = Image.fromarray(image_data)
    else:
        # Data is coming from file upload
        image = Image.open(image_data)

    # Convert the PIL image to bytes
    img_byte_array = io.BytesIO()
    image.save(img_byte_array, format="JPEG")
    image_content = img_byte_array.getvalue()

    # Perform text detection
    image = vision.Image(content=image_content)
    response = client.text_detection(image=image)

    # Extract and return the detected text
    if response.text_annotations:
        text = response.text_annotations[0].description
        return text
    else:
        return "No text found in the image."

# Function to capture an image from the webcam using OpenCV
def capture_image_from_webcam():
    cap = cv2.VideoCapture(0)  # 0 represents the default camera (usually the built-in webcam)

    if not cap.isOpened():
        st.error("Error: Webcam not found. Make sure your camera is connected and try again.")
        return None

    _, frame = cap.read()
    cap.release()  # Release the camera to free it up for other applications
    return frame

# Streamlit app
def main():
    st.title("Text Extraction with Google Cloud Vision API")
    
    # Sidebar for image source selection
    st.sidebar.header("Select Image Source")
    image_source = st.sidebar.radio("Choose an image source:", ("Upload an Image", "Scan Image Using Streamlit cameras Widget"))
    
    if image_source == "Upload an Image":
        uploaded_image = st.file_uploader("Upload an image", type=["jpg", "jpeg", "png"])
        if uploaded_image:
            text = extract_text_from_image(uploaded_image)
            st.image(uploaded_image, caption="Uploaded Image", use_column_width=True)


    elif image_source == "Scan Image Using Streamlit cameras Widget":
        data = st.camera_input("Scan Textual Image")
        if data is not None:
            text = extract_text_from_image(data)

    # Display the extracted text
    if 'text' in locals():
        st.header("Extracted Text")
        st.write(text)

if __name__ == "__main__":
    main()
