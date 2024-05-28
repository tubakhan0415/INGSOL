import streamlit as st
from PIL import Image
import pytesseract
import re

# Set Streamlit theme to dark mode
st.set_page_config(
    page_title="Text Extraction and Search App",
    page_icon="✍️",
    layout="wide",
    initial_sidebar_state="expanded",
)


def main(topic):
    # Your main functionality here
    st.title("Text Extraction and Search App")
    st.write(f"Selected Topic: {topic}")

def style_sidebar(sub, topic):
    # Your sidebar styling code here
    st.sidebar.title(sub)
    st.sidebar.write(f"Topic: {topic}")

def display_source_information():
    # Your source information display code here
    st.write("Source Information: ...")

def extract_text_from_image(data):
    img = Image.open(data)
    return pytesseract.image_to_string(img)

def main_body(sub):
    st.markdown("-----")
    display_source_information()
    st.markdown("-----")
    selection = st.selectbox("Select your option to extract text data from an Image", ['Scan Image Using Streamlit cameras Widget', 'Upload an Image'])
    col1, col2 = st.columns(2)
    data = None
    text = None
    search_term = None  # Initialize search term variable

    with col1:
        if selection == 'Scan Image Using Streamlit cameras Widget':
            data = st.camera_input("Scan Textual Image")
        elif selection == 'Upload an Image':
            data = st.file_uploader('Upload Image', type=['png', 'jpg', 'jpeg'])

    with col2:
        if data:
            st.image(data)
            text = extract_text_from_image(data)

    st.markdown("-----")
    if text:
        st.subheader("Extracted Text")
        st.code(text, language='text')

        st.subheader("Search Text")
        search_term = st.text_input("Enter the word or character to search for:")

        if search_term:
            # Implement the search functionality
            matches = re.finditer(search_term, text, re.IGNORECASE)
            match_count = sum(1 for _ in matches)  # Count of matches

            # Display the search result
            if match_count > 0:
                st.write(f"'{search_term}' found in the extracted text: Yes")
            else:
                st.write(f"'{search_term}' not found in the extracted text: No")

if __name__ == "__main__":
    # Example usage:
    selected_topic = "Text Extraction and Search"
    selected_subtopic = "Image Processing"
    main(selected_topic)
    style_sidebar(selected_subtopic, selected_topic)
    main_body(selected_subtopic)
