from datetime import datetime
import re
import streamlit as st

def trova_data_file(file_name):
    match = re.search(r'(\d{2})_(\d{2})_(\d{4})', file_name)
    if match:
        day, month, year = match.groups()
        # Crea un oggetto datetime per confrontare le date
        formatted_date = f"{day}/{month}/{year}"
        date_obj = datetime.strptime(formatted_date, "%d/%m/%Y")
        print(f"Data estratta per {file_name}: {date_obj}")
        return date_obj
    else:
        print(f"Data non trovata nel nome del file: {file_name}")
        return None

# Lista dei file
files_inventario = [
    "Inventario_01_12_2024.csv",
    "Inventario_14_11_2024.csv",
    "Inventario_15_11_2024.csv",
    "Inventario_16_11_2024.csv"
]

# Trova il file con la data più recente
file_piu_recente_inventario = max(files_inventario, key=trova_data_file)

st.write("Il file con la data più recente è:", file_piu_recente_inventario)