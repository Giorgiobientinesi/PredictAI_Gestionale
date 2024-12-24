from Connessioni_S3 import read_csv_from_s3, upload_dataframe_as_csv, create_directory, list_directory_contents

from dotenv import load_dotenv
import os
import streamlit as st

from Utils import load_inventario

current_directory = os.getcwd()

st.write(current_directory)