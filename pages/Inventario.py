import streamlit as st
from requests.utils import default_user_agent
from streamlit import checkbox
from datetime import datetime, timedelta
import time
from Connessioni_S3 import read_csv_from_s3, upload_dataframe_as_csv, create_directory, list_directory_contents,file_exists_in_s3
import pandas as pd
from Utils import filter_dataframe, trova_data_file, pulisci_vendite_oggi, pulisci_fatture_oggi,aggiorna_vendite_storiche,load_inventario,create_excel_file
from datetime import datetime
from io import BytesIO


st.set_page_config(layout="wide")

with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.title("Inventario Prodotti supermercato")
negozio = "todis-viacastelporziano294"

if "inventario" not in st.session_state:
    st.session_state["inventario"] = pd.DataFrame()

st.logo("assets/Logo.svg",size="large")

# Barra di ricerca
search_text = st.text_input("Cerca prodotto per descrizione:")


# Schede per ciascun valore unico di "Murale"
unique_murales = ["murale-300"]


# Crea il pulsante "Salva tutto"
salva = st.button("Salva tutto")

if salva:
    st.success("Salvato con Successo!")




tabs = st.tabs([el for el in unique_murales])

# Crea una scheda per ciascun "Murale"
for i, murale in enumerate(unique_murales):
    with tabs[i]:
        st.session_state["murale"] = murale
        st.write(st.session_state["murale"])
        # Filtra il DataFrame per il murale corrente
        inventario_df = load_inventario(murale)
        inventario_excel = create_excel_file(inventario_df)
        
        st.download_button(
                    label="Download File Inventario",
                    data=inventario_excel,
                    file_name="Inventario_attuale.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )        
        

        st.session_state["inventario"] = inventario_df

        inventario_df = filter_dataframe(search_text, inventario_df)

        st.session_state["murale"] = murale

        # Gestisci la visualizzazione a righe con 3 prodotti per riga
        num_cols = 3  # Numero di prodotti per riga
        rows = [inventario_df.iloc[i : i + num_cols] for i in range(0, inventario_df.shape[0], num_cols)]

        for row_df in rows:
            cols = st.columns(num_cols)
            for col, (_, row) in zip(cols, row_df.iterrows()):
                with col:
                        # Layout migliorato per visualizzare le informazioni
                        with st.container(border=True):
                            st.write(f"**Key**: {row['Key']}")
                            st.write(f"**Articolo**: {row['Descrizione']}")
                        # Imposta il campo Scaffale come numero intero positivo
                            new_value = st.number_input(
                                "Stock", value=row["Stock"], min_value=0.0, step=0.01, key=f"{row['Key']}_stock"
                            )
                        # Aggiorna il valore nel session_state
                        st.session_state["inventario"].at[row.name, "Stock"] = new_value

if salva:
    today = datetime.today()
    today_formatted = today.strftime("%d/%m/%Y")
    today_formatted = today_formatted.replace("/","_")
    nome_file = "Inventario_"+today_formatted+".csv"
    upload_dataframe_as_csv(st.session_state["inventario"],negozio,st.session_state["murale"]+"/Inventari",nome_file)
    time.sleep(2)
    st.rerun()

