import streamlit as st
from streamlit import checkbox
from datetime import datetime, timedelta
import csv
from Connessioni_S3 import read_csv_from_s3, upload_dataframe_as_csv, create_directory, list_directory_contents
import pandas as pd
from Utils import filter_dataframe, trova_data_file, pulisci_vendite_oggi, pulisci_fatture_oggi, master_job_aggiornamento,aggiorna_vendite_storiche, genera_previsione, pulisci_fattura_oggi
from datetime import datetime
import streamlit_antd_components as sac

st.set_page_config(layout="wide")
st.logo("assets/Logo.svg",size="large")

if "file_temp_vendite" not in st.session_state:
    st.session_state["file_temp_vendite"] = pd.DataFrame()
if "file_temp_acquisti" not in st.session_state:
    st.session_state["file_temp_acquisti"] = pd.DataFrame()
if "aggiorna_view" not in st.session_state:
    st.session_state["aggiorna_view"] = "no"
if "file_temp_vendite_pulito" not in st.session_state:
    st.session_state["file_temp_vendite_pulito"] = pd.DataFrame()
if "uploaded_file_acquisti" not in st.session_state:
    st.session_state["uploaded_file_acquisti"] = 0
if "file_temp_acquisti_pulito" not in st.session_state:
    st.session_state["file_temp_acquisti_pulito"] = 0
if "aggiorna_dati" not in st.session_state:
    st.session_state["aggiorna_dati"] = 0


st.session_state["aggiorna_dati"] = 0

with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.title("Gestisci i file di Vendita e Acquisti qui")
negozio = "todis-viacastelporziano294"

tables = sac.tabs([
    sac.TabsItem(label='murale-300')], align='left')

subtables = sac.tabs([
    sac.TabsItem(label='Vendite'),
    sac.TabsItem(label='Acquisti'),
], align='center')

# Recupero dei murali dal contenuto della directory
directories = list_directory_contents(negozio, "")

st.session_state["murale"] = tables

files_inventario = list_directory_contents(negozio, f"{st.session_state['murale']}/Inventari")
file_piu_recente_inventario = max(files_inventario, key=trova_data_file)
data_piu_recente_inventario = trova_data_file(file_piu_recente_inventario)
data_piu_recente_inventario = data_piu_recente_inventario.date()  # Prendi solo la data


files_vendite = list_directory_contents(negozio, f"{st.session_state['murale']}/Storico_Vendite")
file_piu_recente_storico = max(files_vendite, key=trova_data_file)
data_piu_recente_storico = trova_data_file(file_piu_recente_storico)
data_piu_recente_storico_datetime = data_piu_recente_storico.date()


files_vendite_giornaliere = list_directory_contents(negozio, f"{st.session_state['murale']}/Vendite_giornaliere")
files_acquisti = list_directory_contents(negozio, f"{st.session_state['murale']}/Acquisti_giornalieri")

data_meno_recente = pd.to_datetime(min(data_piu_recente_inventario,data_piu_recente_storico_datetime-timedelta(1)),dayfirst=True)


oggi = datetime.now()


date_intercorse = [(data_meno_recente + timedelta(days=i)).strftime('%d-%m-%Y')
                   for i in range(0,(oggi - data_meno_recente).days + 1)]

date_file_vendite = [trova_data_file(el).strftime("%d-%m-%Y") for el in files_vendite_giornaliere]
date_file_acquisti = [trova_data_file(el).strftime("%d-%m-%Y") for el in files_acquisti]

st.subheader("L'inventario è aggiornato al " + str(data_piu_recente_inventario.strftime("%d-%m-%Y")))

Salva = st.button("Salva tutto")
if Salva:
    st.session_state["aggiorna_view"] = "yes"
    st.rerun()
if subtables=="Vendite":
    for giorno in date_intercorse[:-1]:
        if giorno not in date_file_vendite:
            with st.container(border=True):

                # Crea il file uploader per ciascun giorno
                uploaded_file_vendite = st.file_uploader(f"Carica il file di vendite per il {giorno}", type=["csv"])
                giorno_chiusura = st.checkbox("Negozio chiuso in questo giorno",key = giorno)

                if giorno_chiusura:
                    vendite_zero = pd.DataFrame()
                    vendite_zero["Key"] = []
                    vendite_zero["Descrizione"] = []
                    vendite_zero["Quantità"] = []
                    vendite_zero["ListinoCosto"] = []
                    vendite_zero["ListinoPubblico"] = []
                    vendite_zero["Data"] = []

                    giorno = giorno.replace("-","/")
                    st.session_state["file_temp_vendite"] = vendite_zero
                    st.session_state["file_temp_vendite_pulito"] = vendite_zero
                    giorno = giorno.replace("/","_")
                    nome_file_s3 = f"Vendite_{giorno}.CSV"  # Adatta l'estensione se necessario
                    nome_file_s3_pulito = f"Vendite_{giorno}.csv"  # Adatta l'estensione se necessario

                    st.dataframe(st.session_state["file_temp_vendite_pulito"])
                    upload_dataframe_as_csv(st.session_state["file_temp_vendite"], negozio, "{}/Vendite_giornaliere".format(st.session_state["murale"]), nome_file_s3)
                    upload_dataframe_as_csv(st.session_state["file_temp_vendite_pulito"], negozio, "{}/Vendite_giornaliere_pulite".format(st.session_state["murale"]), nome_file_s3_pulito)

                    st.rerun()


                if uploaded_file_vendite:
                    if uploaded_file_vendite.name.endswith('.CSV'):
                        df = pd.read_csv(uploaded_file_vendite, sep=';', encoding='latin-1')
                        giorno = giorno.replace("-","/")
                        df_pulito = pulisci_vendite_oggi(df,giorno)
                        st.session_state["file_temp_vendite"] = df
                        st.session_state["file_temp_vendite_pulito"] = df_pulito

                        giorno = giorno.replace("/","_")
                        nome_file_s3 = f"Vendite_{giorno}.CSV"  # Adatta l'estensione se necessario
                        nome_file_s3_pulito = f"Vendite_{giorno}.csv"  # Adatta l'estensione se necessario

                        st.dataframe(st.session_state["file_temp_vendite_pulito"])

                        upload_dataframe_as_csv(st.session_state["file_temp_vendite"], negozio, "{}/Vendite_giornaliere".format(st.session_state["murale"]), nome_file_s3)
                        upload_dataframe_as_csv(st.session_state["file_temp_vendite_pulito"], negozio, "{}/Vendite_giornaliere_pulite".format(st.session_state["murale"]), nome_file_s3_pulito)

                        st.rerun()
                    else:
                        st.error("Il file caricato non è valido.")




if subtables == "Acquisti":
    for giorno in date_intercorse[1:]:
        if giorno not in date_file_acquisti:
            if pd.to_datetime(giorno,format="%d-%m-%Y").weekday() != 6:
                giorno = giorno.replace("-", "/")
                with st.container(border=True):
                    uploaded_file_acquisti = st.file_uploader(
                        f"Carica il file di acquisti per il {giorno}", type=["csv"]
                    )
                    num_fattura = st.text_input("Inserisci numero fattura", key=f"fattura_{giorno}")

                    # Create a unique key for each button
                    inserisci_key = f"inserisci_{giorno}"
                    st.divider()


                    giorno_chiusura = st.checkbox("Negozio chiuso in questo giorno",key = giorno)

                    if giorno_chiusura:
                        acquisti_zero = pd.DataFrame()
                        acquisti_zero["Key"] = []
                        acquisti_zero["Descrizione"] = []
                        acquisti_zero["Quantità"] = []
                        acquisti_zero["fattura"] = []

                        st.session_state["file_temp_acquisti_pulito"] = acquisti_zero
                        st.session_state["uploaded_file_acquisti"] = acquisti_zero
                        giorno = giorno.replace("/", "_")
                        nome_file_s3 = f"Acquisti_{giorno}.csv"
                        
                        upload_dataframe_as_csv(
                            st.session_state["file_temp_acquisti_pulito"],
                            negozio,
                            f"{st.session_state['murale']}/Acquisti_giornalieri_puliti",
                            nome_file_s3,
                        )

                        upload_dataframe_as_csv(
                            st.session_state["file_temp_acquisti"],
                            negozio,
                            f"{st.session_state['murale']}/Acquisti_giornalieri",
                            nome_file_s3,
                        )


                    if uploaded_file_acquisti and len(num_fattura) > 0:
                        fatture_esistenti = pd.read_excel("files_utili/fatture.xlsx")
                        fatture_esistenti_list = list(fatture_esistenti["Numeri"].astype(str))

                        if num_fattura in fatture_esistenti_list:
                            st.warning("Fattura già presente nel database. Inserire un'altra fattura.")
                            break
                        else:
                            if uploaded_file_acquisti.name.endswith(".csv"):
                                st.session_state["uploaded_file_acquisti"] = uploaded_file_acquisti
                                try:
                                    df = pd.read_csv(uploaded_file_acquisti, sep=",", encoding='latin-1')
                                except:
                                    df = pd.read_csv(st.session_state["uploaded_file_acquisti"], sep=";", encoding='latin-1')
                                
                                st.session_state["uploaded_file_acquisti"] = df

                                df_pulito = pulisci_fattura_oggi(df)
                                st.session_state["file_temp_acquisti_pulito"] = df_pulito
                                st.session_state["file_temp_acquisti_pulito"]["fattura"] = num_fattura


                                giorno = giorno.replace("/", "_")
                                nome_file_s3 = f"Acquisti_{giorno}.csv"
                                
                                upload_dataframe_as_csv(
                                    st.session_state["file_temp_acquisti_pulito"],
                                    negozio,
                                    f"{st.session_state['murale']}/Acquisti_giornalieri_puliti",
                                    nome_file_s3,
                                )

                                upload_dataframe_as_csv(
                                    st.session_state["uploaded_file_acquisti"],
                                    negozio,
                                    f"{st.session_state['murale']}/Acquisti_giornalieri",
                                    nome_file_s3,
                                )
                                
                                nuova_riga = pd.DataFrame([{"Numeri": num_fattura}])
                                st.write(nuova_riga)
                                fatture_esistenti = pd.concat([fatture_esistenti, nuova_riga], ignore_index=True)
                                fatture_esistenti.to_excel("files_utili/fatture.xlsx",index=False)

                                st.rerun()
                            else:
                                st.error("Il file caricato non è valido.")




