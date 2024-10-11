import streamlit as st
import time
import os

# Static folder paths
FOLDER1_PATH = "/path/to/folder1"
FOLDER2_PATH = "/path/to/folder2"

# Function to get the list of files from a folder
def get_files_in_folder(folder_path):
    try:
        return os.listdir(folder_path)
    except Exception as e:
        return [f"Error reading folder: {str(e)}"]

st.title("Processed Files Monitoring")

# Tabs for each folder
tabs = st.tabs([f"Folder 1 ({FOLDER1_PATH})", f"Folder 2 ({FOLDER2_PATH})"])

# Folder 1 Tab
with tabs[0]:
    st.header(f"Folder 1 - {FOLDER1_PATH}")
    processed_files_display1 = st.empty()

# Folder 2 Tab
with tabs[1]:
    st.header(f"Folder 2 - {FOLDER2_PATH}")
    processed_files_display2 = st.empty()

# Infinite loop to mimic real-time update of processed files
while True:
    # List files in Folder 1
    files_folder1 = get_files_in_folder(FOLDER1_PATH)
    processed_files_display1.table(files_folder1)

    # List files in Folder 2
    files_folder2 = get_files_in_folder(FOLDER2_PATH)
    processed_files_display2.table(files_folder2)

    time.sleep(5)  # Refresh every 5 seconds
