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

# Configurazione della pagina di Streamlit
st.set_page_config(layout="wide")
with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.logo("assets/logo.svg",size="large")

# Inizializzazione dello stato della sessione
if "murale" not in st.session_state:
    st.session_state["murale"] = 0
if "murale_numero" not in st.session_state:
    st.session_state["murale_numero"] = 0
if "vendite_storiche" not in st.session_state:
    st.session_state["vendite_storiche"] = pd.DataFrame()
if "Vendite_oggi" not in st.session_state:
    st.session_state["Vendite_oggi"] = pd.DataFrame()
if "Acquisti_oggi" not in st.session_state:
    st.session_state["Acquisti_oggi"] = pd.DataFrame()
if "inventario" not in st.session_state:
    st.session_state["inventario"] = pd.DataFrame()
if "data_vendite" not in st.session_state:
    st.session_state["data_vendite"] = pd.DataFrame()
if "previsioni" not in st.session_state:
    st.session_state["previsioni"] = pd.DataFrame()
if "anagrafica" not in st.session_state:
    st.session_state["anagrafica"] = pd.DataFrame()
if "anagrafica_scopo" not in st.session_state:
    st.session_state["anagrafica"] = pd.DataFrame()
if "genera_ordine" not in st.session_state:
    st.session_state["genera_ordine"] = pd.DataFrame()
if "aggiorna_dati" not in st.session_state:
    st.session_state["aggiorna_dati"] = 0
if "ordine_finale" not in st.session_state:
    st.session_state["ordine_finale"] = pd.DataFrame()
if "ordine_partito" not in st.session_state:
    st.session_state["ordine_partito"] = 0
if "terminalino" not in st.session_state:
    st.session_state["terminalino"] = 0

# Titolo dell'app
st.title("Genera un Ordine")
negozio = "todis-viacastelporziano294"
today_date = datetime.today().date()



with st.spinner("Aggiornando i Dati. Prego Attendere!"):
    if st.session_state["aggiorna_dati"] != 1:
        master_job_aggiornamento()
        st.session_state["aggiorna_dati"] = 1


# Recupero dei murali dal contenuto della directory
directories = list_directory_contents(negozio, "")
murali = [el.split("-")[-1] for el in directories if el != "Anagrafica" and el != "Inventario"]
murali.append("")

# Selezione del murale
with st.container(border=True):
    murale = st.selectbox("Scegli il Murale per il quale vuoi generare un Ordine!", murali,index=1)
    st.session_state["murale_numero"] = murale
    st.session_state["murale"] = f"murale-{murale}"

if st.session_state["murale_numero"] != "":
    files_inventario = list_directory_contents(negozio, f"{st.session_state['murale']}/Inventari")
    file_piu_recente_inventario = max(files_inventario, key=trova_data_file)
    data_piu_recente_inventario = trova_data_file(file_piu_recente_inventario)
    data_piu_recente_inventario = data_piu_recente_inventario.date()  # Prendi solo la data
    ultimo_inventario = read_csv_from_s3(
        negozio,
        f"{st.session_state['murale']}/Inventari",
        file_piu_recente_inventario,
        ","
    )

    ultimo_inventario.loc[ultimo_inventario["Stock"] < 0, "Stock"] = 0

    st.subheader("L'inventario è aggiornato al " + data_piu_recente_inventario.strftime("%d-%m-%Y"))

    st.session_state["inventario"] = ultimo_inventario


    # Step 2: Carica lo storico delle vendite
    files_vendite = list_directory_contents(negozio, f"{st.session_state['murale']}/Storico_Vendite")
    file_piu_recente_storico = max(files_vendite, key=trova_data_file)
    data_piu_recente_storico = trova_data_file(file_piu_recente_storico)
    data_piu_recente_storico_datetime = data_piu_recente_storico.date()
    vendite_storiche = read_csv_from_s3(
        negozio,
        f"{st.session_state['murale']}/Storico_Vendite",
        file_piu_recente_storico,
        ","
        )

    st.session_state["vendite_storiche"] = vendite_storiche


    locale.setlocale(locale.LC_TIME, "it_IT.UTF-8")
    oggi = datetime.now().date()
    nome_giorno = oggi.strftime("%A")


    if data_piu_recente_inventario != oggi:
        st.warning("L'inventario non è aggiornato a oggi. Vai su file manager ad aggiornare i dati di vendita e di acquisto!")
    if (data_piu_recente_storico+timedelta(1)).date() != oggi:
        st.warning("Le Vendite non sono aggiornate alla chisurua della giornata di ieri. Vai su file manager ad aggiornare i dati!")






    col1,col2,col3 = st.columns(3)
    if data_piu_recente_inventario == oggi and data_piu_recente_storico.date() == oggi-timedelta(1) :
        # Step 4: Carica l'anagrafica
        anagrafica = read_csv_from_s3(negozio, "Anagrafica", "Anagrafica.csv",delimiter=",")
        st.session_state["anagrafica"] = anagrafica
        st.session_state["anagrafica_scopo"] = st.session_state["anagrafica"][st.session_state["anagrafica"]["Murale"] == int(st.session_state["murale_numero"])]

        with col1.container(border=True):
            in_scopo_lista = st.session_state["anagrafica"]["Descrizione"].tolist()
            in_scopo_lista.append("Tutti")
            in_scopo = st.multiselect(
                "Scegli i prodotti in scopo dell'ordine.",
                in_scopo_lista,
                default="Tutti"
            )

            if "Tutti" in in_scopo:
                # Non filtrare: mantieni tutto invariato
                pass
            else:
                # Filtra in base alla selezione
                st.session_state["anagrafica_scopo"] = st.session_state["anagrafica_scopo"][
                    st.session_state["anagrafica_scopo"]["Descrizione"].isin(in_scopo)
                ]
        with col2.container(border=True):
            fuori_scopo_lista = st.session_state["anagrafica"]["Descrizione"].tolist()
            fuori_scopo_lista.append("Nessuno")
            fuori_scopo_lista = st.multiselect(
                "Scegli i prodotti da escludere nell'ordine.",
                fuori_scopo_lista,
                default="Nessuno"
            )

            if "Nessuno" in in_scopo:
                # Non filtrare: mantieni tutto invariato
                pass
            else:
                # Filtra in base alla selezione
                st.session_state["anagrafica_scopo"] = st.session_state["anagrafica_scopo"][
                    ~st.session_state["anagrafica_scopo"]["Descrizione"].isin(fuori_scopo_lista)
                ]

        st.write(st.session_state["anagrafica_scopo"])

        index_giorno = 0 if nome_giorno == "Venerdì" else 1
        with col3.container(border=True):
            weekend = st.selectbox("L'ordine comprende il weekend?",["Si","No"],index=index_giorno)

        giorni_previsionali = 4 if weekend == "No" else 6

        genera_previsione = st.button("Ordine")

        if genera_previsione:
            st.session_state["ordine_partito"] = 1
            chiavi = []
            descrizioni = []
            previsioni = []
            # Filtra per il murale specificato
            Murale = st.session_state["anagrafica_scopo"]
            st.session_state["offerte"] = read_csv_from_s3(negozio, f"{st.session_state['murale']}", "Promozioni.csv",
                                                           ",")
            st.session_state["offerte"] = st.session_state["offerte"][
                (pd.to_datetime(st.session_state["offerte"]["Data_inizio"]) <= pd.to_datetime(oggi)) &
                (pd.to_datetime(st.session_state["offerte"]["Data_fine"]) >= pd.to_datetime(oggi))
                ]

            for key in Murale["Key"]:

                if key in list(vendite_storiche["Key"]):
                    vendite_prodotto = st.session_state["vendite_storiche"][st.session_state["vendite_storiche"]["Key"] == key].reset_index(drop=True)
                    descrizione_prodotto = vendite_prodotto["Descrizione"].iloc[0]
                    if key in list(st.session_state["offerte"]["Key"]):
                        giorni_previsionali +=2

                    scaffale = Murale[Murale["Key"]==key]["Scaffale"].iloc[0]
                    if scaffale == 999:
                        vendite_prodotto["Data"] = pd.to_datetime(vendite_prodotto["Data"].str.split(" ").str[0])
                        # Crea un intervallo di date completo fino ad oggi
                        date_range = pd.date_range(start='2024-10-01',
                                                   end=oggi)  # i giorni senza vendita devono essere pari a 0
                        full_dates = pd.DataFrame({'Data': date_range})
                        merged_df = pd.merge(full_dates, vendite_prodotto, on='Data', how='left')
                        merged_df['Quantità'] = merged_df['Quantità'].fillna(0)
                        merged_df['Key'] = merged_df['Key'].fillna(
                            key)  # merged_df è la serie storica di vendite senza giorni mancanti

                        merged_df.rename(columns={'Data': 'ds', "Quantità": "y"}, inplace=True)
                        merged_df = merged_df[["ds", "y"]]

                        merged_df["y"] = merged_df["y"].str.replace(",",".").astype(float)

                        threshold = merged_df['y'].quantile(0.9)
                        merged_df['y'] = merged_df['y'].apply(lambda x: 0.9 if x > threshold else x)

                        try:
                            m = Prophet(interval_width=0.5, weekly_seasonality=True, changepoint_prior_scale=0.05,
                                        seasonality_prior_scale=0.1)
                            model = m.fit(merged_df)
                            future = m.make_future_dataframe(periods=giorni_previsionali, freq='d')
                            prophet_forecast = m.predict(future)

                            previsione = sum(prophet_forecast["yhat"].iloc[-giorni_previsionali:])
                        except:
                            st.write("Errore Prophet")
                            previsione = sum(merged_df['y'].rolling(window=50).mean().tail(giorni_previsionali))
                        #st.write(previsione)
                        if previsione > 0:
                            previsioni.append(previsione)
                        else:
                            previsioni.append(0)

                    else:
                        previsioni.append(scaffale)

                    chiavi.append(key)
                    descrizioni.append(descrizione_prodotto)

            Dataframe_previsioni = pd.DataFrame()
            Dataframe_previsioni["Key"] = chiavi
            Dataframe_previsioni["Descrizione"] = descrizioni
            Dataframe_previsioni["Previsione"] = previsioni


            def nearest_multiple_of_minimo(x):
                if x <= 0:
                    return 0
                # Round up to the nearest multiple of minimo_ordine
                rounded_value = max(minimo_ordine, math.ceil(x / minimo_ordine) * minimo_ordine)
                # Return the rounded value but not exceeding max_ordine
                return rounded_value

            chiavi = []
            descrizioni = []
            imballaggi = []
            pacchi_da_ordinare = []
            inventario = []
            previsioni_lista = []

            for key in list(Dataframe_previsioni["Key"]):
                if key in list(st.session_state["inventario"]["Key"]):
                    stock_scaffale = float(st.session_state["inventario"][st.session_state["inventario"]["Key"] == key]["Stock"])
                    previsioni = float(Dataframe_previsioni[Dataframe_previsioni["Key"] == key]["Previsione"])
                    minimo_ordine = st.session_state["anagrafica_scopo"][st.session_state["anagrafica_scopo"]["Key"] == key]["Imb."].iloc[0]

                    if minimo_ordine == None:
                        minimo_ordine=1

                    descrizione = st.session_state["anagrafica_scopo"][st.session_state["anagrafica_scopo"]["Key"] == key]["Descrizione"].iloc[0]

                    if previsioni < stock_scaffale:
                        ordine = 0
                    else:
                        ordine = previsioni - stock_scaffale


                    ordine = nearest_multiple_of_minimo(ordine)

                    if ordine + stock_scaffale < minimo_ordine:
                        ordine = minimo_ordine

                    ordine = nearest_multiple_of_minimo(ordine)
                    ordine = ordine/minimo_ordine

                    chiavi.append(key)
                    descrizioni.append(descrizione)
                    imballaggi.append(minimo_ordine)
                    pacchi_da_ordinare.append(ordine)
                    inventario.append(stock_scaffale)
                    previsioni_lista.append(previsioni)


            genera_ordine = pd.DataFrame()
            genera_ordine["Key"] = chiavi
            genera_ordine["Chiave"] = descrizioni
            genera_ordine["Imballaggi"] = imballaggi
            genera_ordine["pacchi_da_ordinare"] = pacchi_da_ordinare
            genera_ordine["inventario"] = inventario
            genera_ordine["previsioni_lista"] = previsioni_lista


            st.session_state["genera_ordine"] = genera_ordine


        st.divider()

        if st.session_state["ordine_partito"] == 1:
            with st.popover("Visualizza e Modifica ordine",use_container_width=True):
                grid_options = GridOptionsBuilder.from_dataframe(st.session_state["genera_ordine"])
                grid_options.configure_column('pacchi_da_ordinare', editable=True)  # You can enable specific columns

                # Display the editable DataFrame
                grid_response = AgGrid(st.session_state["genera_ordine"], gridOptions=grid_options.build(), editable=True)

                st.session_state["genera_ordine"] = grid_response['data']

                ordine_automatico = terminalino(st.session_state["genera_ordine"])
                output = StringIO()
                for line in ordine_automatico['formattato']:
                    output.write(line + '\n')
                output = output.getvalue()

                # Fornisci il file scaricabile
                st.download_button(
                    label="Download File Terminalino",
                    data=output,
                    file_name="ordine_automatico.txt",
                    mime="text/plain"
                )

                excel_file = create_excel_file(st.session_state["genera_ordine"])

                st.download_button(
                    label="Download Excel File",
                    data=excel_file,
                    file_name="PredictAI_ordine.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )









