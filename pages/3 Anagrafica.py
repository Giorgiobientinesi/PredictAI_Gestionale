import streamlit as st
from Connessioni_S3 import read_csv_from_s3,upload_dataframe_as_csv,create_directory,list_directory_contents
import pandas as pd
import time
from Utils import filter_dataframe, trova_data_file, pulisci_vendite_oggi, pulisci_fatture_oggi,aggiorna_vendite_storiche,load_inventario
from datetime import datetime

st.set_page_config(layout="wide")

if "anagrafica" not in st.session_state:
    st.session_state["anagrafica"] = pd.DataFrame()
if "murale" not in st.session_state:
    st.session_state["murale"] = "murale-300"
if 'Light' not in st.session_state:
    st.session_state['Light'] = 'red'

st.logo("assets/Logo.svg",size="large")

with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


if st.session_state['Light'] == 'green':
    st.title("Anagrafica Prodotti supermercato")
    negozio = "todis-viacastelporziano294"



    murali = list_directory_contents(negozio,"Anagrafica")

    anagrafica = read_csv_from_s3(negozio,"Anagrafica","Anagrafica.csv",delimiter=",")  #qua ci deve stare l'anagrafica generica di tutti i murali

    try:
        if not anagrafica or anagrafica.empty:
            anagrafica = read_csv_from_s3(negozio, "Anagrafica", "Anagrafica.csv",
                                        delimiter=";")  # qua ci deve stare l'anagrafica generica di tutti i murali
    except:
        pass

    st.session_state["anagrafica"] = anagrafica

    st.session_state["anagrafica"] = st.session_state["anagrafica"].drop_duplicates(subset="Key", keep="first")

    st.session_state["anagrafica"]["Imb."] = st.session_state["anagrafica"]["Imb."].fillna(0) #QUESTO CI DEVE GIA ESSERE

    # Barra di ricerca
    search_text = st.text_input("Cerca prodotto per descrizione:")
    # Filtra il DataFrame in base al testo della ricerca
    filtered_df = filter_dataframe(search_text,st.session_state["anagrafica"])


    if len(filtered_df)> 0:
        # Schede per ciascun valore unico di "Murale"
        unique_murales = filtered_df["Murale"].unique()


        with st.popover("Aggiungi un Prodotto"):  #Questo ragiona in maniera separata dal resto e serve ad aggiornare l'anagrafica
            st.markdown("Stai aggiungendo un prodotto 👋")
            Key_articolo = st.text_input("Inserisci Chiave Articolo")
            Descrizione_articolo = st.text_input("Inserisci Descrizione Articolo")
            UnitàXpacco_articolo = st.number_input("Inserisci le Unità x Pacco",min_value=1,step=1)
            murale_articolo = st.selectbox("Murale di Appartenenza",unique_murales)
            Scaffale_articolo = st.number_input("Inserisci quanta quantità ne vuoi in Scaffale. Lasciare 999 per default predict.",value=999)
            Prezzo_Acquisto_articolo = st.number_input("Inserisci il Prezzo di Acquisto",min_value=0.0,step=0.01)
            Prezzo_Vendita_articolo = st.number_input("Inserisci il Prezzo di Vendita",min_value=0.0,step=0.01)

            aggiungi_articolo = st.button("A")


            if aggiungi_articolo:
                nuovo_prodotto = pd.DataFrame([[Key_articolo,Descrizione_articolo,UnitàXpacco_articolo,Scaffale_articolo,murale_articolo,Prezzo_Acquisto_articolo,Prezzo_Vendita_articolo]],columns=filtered_df.columns)
                if Key_articolo not in list(st.session_state["anagrafica"]["Key"]):
                    st.session_state["anagrafica"] = pd.concat([st.session_state["anagrafica"],nuovo_prodotto])
                    st.session_state["anagrafica"]["Murale"] = st.session_state["anagrafica"]["Murale"].astype(str).str.split(".").str[0]
                    upload_dataframe_as_csv(st.session_state["anagrafica"], negozio, "Anagrafica", "Anagrafica.csv")

                    st.session_state['murale'] = "murale-"+str(murale_articolo)

                    files_inventario = list_directory_contents(negozio, f"{st.session_state['murale']}/Inventari")
                    file_piu_recente_inventario = max(files_inventario, key=trova_data_file)


                    ultimo_inventario = load_inventario(st.session_state['murale'])

                    if Key_articolo not in list(ultimo_inventario["Key"]):
                        inventario_to_add = pd.DataFrame(
                            [[Key_articolo, Descrizione_articolo,"PZ",0]],
                            columns=["Key", "Descrizione","UM", "Stock"]
                        )
                        ultimo_inventario = pd.concat([ultimo_inventario, inventario_to_add], ignore_index=True)
                        upload_dataframe_as_csv(ultimo_inventario, negozio,
                                                st.session_state["murale"] + "/Inventari", file_piu_recente_inventario)

                    st.toast("Anagrafica e Inventario aggiornata correttamente!")
                    time.sleep(5)
                    st.rerun()
                else:
                    st.error("Il prodotto è già presente nell'anagrafica. Usa la barra di ricerca per vederlo.")

        # Crea il pulsante "Salva tutto"
        salva = st.button("Salva tutto")

        if salva:
            st.success("Salvato con Successo!")


        tabs = st.tabs([f"Murale {murale}" for murale in unique_murales])




        # Crea una scheda per ciascun "Murale"
        for i, murale in enumerate(unique_murales):
            with tabs[i]:
                # Filtra il DataFrame per il murale corrente
                murale_df = filtered_df[filtered_df["Murale"] == murale]
                st.session_state["murale"] = "murale-" +str(murale)
                # Gestisci la visualizzazione a righe con 3 prodotti per riga
                num_cols = 3  # Numero di prodotti per riga
                rows = [murale_df.iloc[i : i + num_cols] for i in range(0, murale_df.shape[0], num_cols)]

                for row_df in rows:
                    cols = st.columns(num_cols)
                    for col, (_, row) in zip(cols, row_df.iterrows()):
                        with col:
                            with st.expander(f"### **{row['Descrizione']}**", expanded=False):
                                # Layout migliorato per visualizzare le informazioni
                                with st.container(border=True):
                                    st.markdown(f" Unità Imballo:  {row['Imb.']}")
                                with st.container(border=True):
                                    st.write(f"**Key**: {row['Key']}")
                                with st.container(border=True):
                                    st.write(f"**Prezzo di Acquisto**: {row['Prezzo Acquisto']}")
                                with st.container(border=True):
                                    st.write(f"**Prezzo di Vendita**: {row['Prezzo Vendita']}")
                                # Imposta il campo Scaffale come numero intero positivo
                                with st.container(border=True):
                                    new_value = st.number_input(
                                        "Scaffale", value=row["Scaffale"], min_value=0, step=1, key=f"{row['Key']}_scaffale"
                                    )


                                # Aggiorna il valore nel session_state
                                st.session_state["anagrafica"].at[row.name, "Scaffale"] = new_value

                                st.markdown("<span style='color:darkred; font-weight:bold;'>**Elimina prodotto**</span>", unsafe_allow_html=True)
                                delete = st.button("X", key=f'Elimina prodotto_{row["Key"]}')



                                if delete:
                                    st.write(st.session_state["murale"])
                                    st.session_state["anagrafica"].drop(row.name, inplace=True)
                                    upload_dataframe_as_csv(st.session_state["anagrafica"], negozio, "Anagrafica",
                                                            "Anagrafica.csv")


                                    files_inventario = list_directory_contents(negozio,
                                                                            f"{st.session_state['murale']}/Inventari")

                                    file_piu_recente_inventario = max(files_inventario, key=trova_data_file)

                                    ultimo_inventario = load_inventario(st.session_state['murale'])

                                    if row['Key'] in list(ultimo_inventario["Key"]):
                                        ultimo_inventario = ultimo_inventario[ultimo_inventario["Key"] != row['Key']]

                                        upload_dataframe_as_csv(ultimo_inventario, negozio,
                                                                st.session_state["murale"] + "/Inventari", file_piu_recente_inventario)

                                    st.toast("Prodotto cancellato correttamente!")
                                    time.sleep(2)
                                    st.rerun()



        if salva:
            upload_dataframe_as_csv(st.session_state["anagrafica"],negozio,"Anagrafica","Anagrafica.csv")
            time.sleep(2)
            st.rerun()

    else:
        st.error("Nessun prodotto trovato. Prova a cambiare la tua ricerca.")  
else:
    st.subheader("Vai a pagina Login per accedere")