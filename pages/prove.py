from Connessioni_S3 import read_csv_from_s3, upload_dataframe_as_csv, create_directory, list_directory_contents

from dotenv import load_dotenv
import os
import streamlit as st

from Utils import load_inventario

load_dotenv()

st.write(os.getenv('AWS_ACCESS_KEY_ID'))

inventario = load_inventario(murale="murale-300")