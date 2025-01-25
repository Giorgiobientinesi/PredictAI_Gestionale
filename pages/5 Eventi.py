import streamlit as st
from pygments.lexer import default
from streamlit import checkbox
import math
from Connessioni_S3 import read_csv_from_s3, upload_dataframe_as_csv, create_directory, list_directory_contents
import pandas as pd
from Utils import filter_dataframe, trova_data_file, pulisci_vendite_oggi, pulisci_fatture_oggi,aggiorna_vendite_storiche, genera_previsione,master_job_aggiornamento,mostra_promozioni
from datetime import datetime, timedelta
from prophet import Prophet
import locale
from st_aggrid import AgGrid, GridOptionsBuilder
from datetime import date

# Configurazione della pagina di Streamlit
st.set_page_config(layout="wide")
with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.logo("assets/Logo.svg",size="large")


if "anagrafica" not in st.session_state:
    st.session_state["anagrafica"] = pd.DataFrame()
if "offerte" not in st.session_state:
    st.session_state["offerte"] = pd.DataFrame()
if "murale" not in st.session_state:
    st.session_state["murale"] = 0
if 'Light' not in st.session_state:
    st.session_state['Light'] = 'red'

if st.session_state['Light'] == 'green':
    negozio = "todis-viacastelporziano294"

    # Titolo dell'app
    st.title("Crea una Promozione")


    # Recupero dei murali dal contenuto della directory
    directories = list_directory_contents(negozio, "")
    murali = [el.split("-")[-1] for el in directories if el != "Anagrafica" and el != "Inventario"]
    murale = st.selectbox("Scegli il Murale di referenza", murali, index=0)
    st.session_state["murale"] = f"murale-{murale}"
    anagrafica = read_csv_from_s3(negozio, "Anagrafica", "Anagrafica.csv", delimiter=",")  # qua ci deve stare l'anagrafica generica di tutti i murali

    
    st.session_state["offerte"] = read_csv_from_s3(negozio, f"{st.session_state['murale']}", "Promozioni.csv", ",")
    if len(st.session_state["offerte"].columns) < 2:
        st.session_state["offerte"] = read_csv_from_s3(negozio, f"{st.session_state['murale']}", "Promozioni.csv", ";")


    try:
        if not anagrafica or anagrafica.empty:
            anagrafica = read_csv_from_s3(negozio, "Anagrafica", "Anagrafica.csv", delimiter = ";")
    except:
        pass


    st.session_state["anagrafica"] = anagrafica[anagrafica["Murale"] == int(murale)]

    tabs = st.tabs(["Crea Promozione","Visualizza Promozioni"])

    with tabs[0]:
        with st.container(border=True):

            chiave_prodotto = st.multiselect(
                "Inserisci la chiave prodotto:",
                options=st.session_state["anagrafica"]['Key'],
                default=None
            )

            descrizione_prodotti = st.session_state["anagrafica"].set_index('Key').loc[chiave_prodotto]['Descrizione']

            if descrizione_prodotti.empty:
               descrizione_prodotti = st.session_state["anagrafica"]['Descrizione']

            prodotti_selezionati = st.multiselect(
                "Seleziona i prodotti per la promozione:",
                options=descrizione_prodotti,
                default=None
            )
            data_inizio = st.date_input(
                "Seleziona la data di inizio della promozione:",
                min_value=date.today()
            )

            # Selezione della data di fine
            data_fine = st.date_input(
                "Seleziona la data di fine della promozione:",
                min_value=data_inizio
            )
            if st.button("Crea Promozione"):
                if not prodotti_selezionati:
                    st.error("Devi selezionare almeno un prodotto per creare una promozione.")
                elif data_fine < data_inizio:
                    st.error("La data di fine deve essere uguale o successiva alla data di inizio.")
                else:
                    id = []
                    count = st.session_state['offerte']["ID"].max()
                    for i in prodotti_selezionati:
                        count += 1
                        id.append(int(count))

                    promozione_df = pd.DataFrame({
                        'ID': id,
                        'Descrizione': prodotti_selezionati,
                        'Data_inizio': [data_inizio] * len(prodotti_selezionati),
                        'Data_fine': [data_fine] * len(prodotti_selezionati)
                    })

                    st.success("Promozione creata con successo!")
                    st.dataframe(promozione_df)

                    st.session_state["anagrafica"] = st.session_state["anagrafica"].drop_duplicates(subset="Descrizione")

                    promozioni_to_upload = pd.merge(
                        promozione_df,
                        st.session_state["anagrafica"][["Key","Descrizione"]],
                        on="Descrizione",
                        how="left"
                    )

                    promozioni_to_upload = promozioni_to_upload[["ID", "Key","Descrizione","Data_inizio","Data_fine"]]
                    st.session_state["offerte"] = pd.concat([st.session_state["offerte"],promozioni_to_upload],axis=0)
                    upload_dataframe_as_csv(st.session_state["offerte"], negozio,
                                            st.session_state["murale"], "Promozioni.csv")

                    st.success("Promozioni aggiornata nel sistema!")
                    st.rerun()

    with tabs[1]:
        # Converti le date in formato datetime
        st.session_state["offerte"]["Data_inizio"] = pd.to_datetime(st.session_state["offerte"]["Data_inizio"])
        st.session_state["offerte"]["Data_fine"] = pd.to_datetime(st.session_state["offerte"]["Data_fine"])

        # Data di oggi
        oggi = datetime.today()

        # Filtra promozioni attive oggi
        promozioni_attive = st.session_state["offerte"][(st.session_state["offerte"]["Data_inizio"] <= oggi) & (st.session_state["offerte"]["Data_fine"] >= oggi)]

        # Filtra promozioni future
        promozioni_future = st.session_state["offerte"][st.session_state["offerte"]["Data_inizio"] > oggi]

        # Promozioni attive oggi
        with st.container(border=True):
            st.markdown('<h3 class="today">Promozioni attive oggi</h3>', unsafe_allow_html=True)
            mostra_promozioni(promozioni_attive, "attive")

        # Promozioni future
        with st.container(border=True):
            st.markdown('<h3 class="tomorrow">Promozioni future</h3>', unsafe_allow_html=True)
            mostra_promozioni(promozioni_future, "future")


else:
    st.subheader("Vai a pagina login per accedere")