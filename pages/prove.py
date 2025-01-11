from Connessioni_S3 import read_csv_from_s3, upload_dataframe_as_csv, create_directory, list_directory_contents

from dotenv import load_dotenv
import os
import streamlit as st
import streamlit as st
from pygments.lexer import default
from streamlit import checkbox
import math
from Connessioni_S3 import read_csv_from_s3, upload_dataframe_as_csv, create_directory, list_directory_contents
import pandas as pd
from Utils import filter_dataframe, trova_data_file, pulisci_vendite_oggi, pulisci_fatture_oggi,aggiorna_vendite_storiche, genera_previsione,master_job_aggiornamento,terminalino,create_excel_file
from datetime import datetime, timedelta
from prophet import Prophet
import locale
from st_aggrid import AgGrid, GridOptionsBuilder
from io import StringIO
from Utils import load_inventario
negozio = "todis-viacastelporziano294"
today_date = datetime.today().date()
current_directory = os.getcwd()

anagrafica = read_csv_from_s3(negozio, "Anagrafica", "Anagrafica.csv",delimiter=",")
st.session_state["anagrafica"] = anagrafica

anagrafica = read_csv_from_s3(negozio,"Anagrafica","Anagrafica.csv",delimiter=",")  #qua ci deve stare l'anagrafica generica di tutti i murali

try:
    if not anagrafica or anagrafica.empty:
        anagrafica = read_csv_from_s3(negozio, "Anagrafica", "Anagrafica.csv",
                                      delimiter=";")  # qua ci deve stare l'anagrafica generica di tutti i murali
except:
    pass
#st.session_state["anagrafica_scopo"] = st.session_state["anagrafica"][st.session_state["anagrafica"]["Murale"] == int(st.session_state["murale_numero"])]