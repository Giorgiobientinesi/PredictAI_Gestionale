import re
import pandas as pd
from datetime import date
import datetime
from prophet import Prophet
from prophet.diagnostics import cross_validation
from prophet.diagnostics import performance_metrics
import streamlit as st
from Connessioni_S3 import read_csv_from_s3, upload_dataframe_as_csv, create_directory, list_directory_contents,file_exists_in_s3
from datetime import datetime, timedelta
from io import StringIO,BytesIO  # Import StringIO for in-memory file handling

#MODIFICA ANAGRAFICA
def filter_dataframe(search_text, df):
    if search_text:
        # Filtra sia sulla colonna 'Descrizione' che su 'Key'
        mask = df["Descrizione"].str.contains(search_text, case=False) | df["Key"].str.contains(search_text, case=False)
        return df[mask]
    else:
        return df

#GENERA ORDINE




def pulisci_vendite_oggi(file,data_di_oggi):
    #file = pd.read_csv(file, sep=';', encoding='latin-1')
    file["Data"] = data_di_oggi
    file = file.rename(columns=lambda x: x.replace(' ', ''))
    file = file.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    file['Articolo'] = file['Articolo'].astype(str)
    file['Articolo'] = file['Articolo'].str.replace(' ', '', regex=True).replace('', pd.NA)
    file = file.dropna(subset=['Articolo'])
    file = file.dropna(subset=['Variante'])
    file['Articolo'] = file['Articolo'].astype(int)
    file['Variante'] = file['Variante'].astype(int)
    file["Key"] = file["Articolo"].astype("str") + "-" + file["Variante"].astype("str")
    file['Data'] = file['Data'].astype(str).str.strip()
    file['Data'] = pd.to_datetime(file['Data'], format='%d/%m/%Y')
    file = file[file['Quantità'].str.len() != 0]
    file['Quantità'] = file['Quantità'].str.strip()
    file['Quantità'] = file['Quantità'].str.replace(',', '.').astype(float).astype(float)
    file = file[["Key", "Descrizione", "Quantità","ListinoCosto","ListinoPubblico","Data"]]
    return file

def pulisci_fatture_oggi(files):
    totale_acquisti_oggi = pd.DataFrame()
    for fattura in files:
        acquisti_oggi = pd.read_csv(fattura,sep=';',encoding='latin-1')
        acquisti_oggi = acquisti_oggi.dropna(subset=['Articolo'])
        acquisti_oggi["Cod."] = acquisti_oggi["Cod."].astype(int)
        acquisti_oggi["Diff."] = acquisti_oggi["Diff."].astype(int)
        acquisti_oggi["Key"] = acquisti_oggi["Cod."].astype("str") + "-" + acquisti_oggi["Diff."].astype("str")
        acquisti_oggi.rename(columns={'Qta': 'Quantità'}, inplace=True)
        acquisti_oggi['Data'] = pd.to_datetime(acquisti_oggi['Data'], format='%d/%m/%y')
        acquisti_oggi['Data'] = acquisti_oggi['Data'].dt.strftime('%Y/%m/%d')
        acquisti_oggi['Quantità'] = acquisti_oggi['Quantità'].str.replace(',', '.').str.strip().astype(float)
        totale_acquisti_oggi = pd.concat([totale_acquisti_oggi,acquisti_oggi])
        totale_acquisti_oggi = totale_acquisti_oggi[["Key","Descrizione","Quantità"]]

        return totale_acquisti_oggi

def pulisci_fattura_oggi(file):
    try:
        acquisti_oggi = file.dropna(subset=['Articolo'])
        acquisti_oggi.columns = acquisti_oggi.columns.str.replace(' ', '')
        acquisti_oggi['Cod.'] = acquisti_oggi['Cod.'].str.replace('.', '', regex=False)
        acquisti_oggi['Cod.'] = acquisti_oggi['Cod.'].str.replace(',', '.', regex=False).astype(float).astype(int)
        acquisti_oggi['Diff.'] = acquisti_oggi['Diff.'].str.replace('.', '', regex=False)
        acquisti_oggi['Diff.'] = acquisti_oggi['Diff.'].str.replace(',', '.', regex=False).astype(float).astype(int)
        acquisti_oggi["Diff."] = acquisti_oggi["Diff."].astype(int)
        acquisti_oggi["Key"] = acquisti_oggi["Cod."].astype("str") + "-" + acquisti_oggi["Diff."].astype("str")
        acquisti_oggi.rename(columns={'Qta': 'Quantità'}, inplace=True)
        acquisti_oggi["Data"] = pd.to_datetime(
            acquisti_oggi["Data"],
            format="%d/%m/%y",  # Specifica il formato atteso
            errors="coerce",  # Sostituisce i valori non validi con NaT (Not a Time)
            dayfirst=True  # Specifica che il giorno è il primo elemento
        )

        acquisti_oggi['Data'] = acquisti_oggi['Data'].dt.strftime('%Y/%m/%d')
        acquisti_oggi['Quantità'] = acquisti_oggi['Quantità'].str.replace(',', '.').str.strip().astype(float)
        acquisti_oggi = acquisti_oggi[["Key", "Descrizione", "Quantità"]]
        acquisti_oggi = acquisti_oggi.groupby(["Key", "Descrizione"], as_index=False).sum()
        return acquisti_oggi
    except:
        acquisti_oggi = acquisti_oggi.dropna(subset=['Articolo'])
        acquisti_oggi['Cod.'] = acquisti_oggi['Cod.'].fillna("").astype(str)
        acquisti_oggi['Cod.'] = pd.to_numeric(acquisti_oggi['Cod.'], errors='coerce').astype(int)
        acquisti_oggi['Diff.'] = acquisti_oggi['Diff.'].fillna("").astype(str)
        acquisti_oggi['Diff.'] = pd.to_numeric(acquisti_oggi['Diff.'], errors='coerce').astype(int)

        acquisti_oggi["Key"] = acquisti_oggi["Cod."].astype("str") + "-" + acquisti_oggi["Diff."].astype("str")
        acquisti_oggi.rename(columns={'Qta': 'Quantità'}, inplace=True)
        acquisti_oggi = acquisti_oggi[["Key", "Data", "N.Imb.", "Quantità"]]
        # acquisti_oggi['Data'] = pd.to_datetime(acquisti_oggi['Data'], format='%d/%m/%y', errors='coerce')
        # acquisti_oggi['Data'] = acquisti_oggi['Data'].dt.strftime('%Y/%m/%d')
        acquisti_oggi['Quantità'] = acquisti_oggi['Quantità'].str.replace(',', '.').str.strip().astype(float)
        acquisti_oggi['N.Imb.'] = acquisti_oggi['N.Imb.'].str.replace(',', '.').str.strip().astype(float)

        return acquisti_oggi


def aggiorna_vendite_storiche(vendite_storiche,vendite_oggi,oggi):
    if len(vendite_storiche[
               vendite_storiche["Data"] == oggi]) == 0:  # Se non sono gia state aggiornate con le vendite di oggi
        vendite_aggiornate = pd.concat([vendite_storiche, vendite_oggi], axis=0)  # Aggiorna con i dati di oggi
    else:
        print("I dati di vendita di ieri sono già registrati nel DB delle vendite storiche.")

    vendite_aggiornate = vendite_aggiornate.drop_duplicates()
    return vendite_aggiornate



# Funzione Prophet
def Prophet_modello_Todis(df):
    param_grid = {
        'changepoint_prior_scale': [0.1],
        'seasonality_prior_scale': [0.1]
    }

    # Iterate over parameter combinations
    best_params = None
    best_mse = float('inf')

    for changepoint_prior in param_grid['changepoint_prior_scale']:
        for seasonality_prior in param_grid['seasonality_prior_scale']:
            # Instantiate and fit the model
            model = Prophet(changepoint_prior_scale=changepoint_prior, seasonality_prior_scale=seasonality_prior)
            model.fit(df)  # Assuming 'df' is your time series DataFrame

            # Perform cross-validation and evaluate performance
            df_cv = cross_validation(model, initial='15 days', period='5 days', horizon='5 days')
            df_p = performance_metrics(df_cv)
            mse = df_p['mse'].values[0]

            # Check if the current combination is the best so far
            if mse < best_mse:
                best_mse = mse
                best_params = {'changepoint_prior_scale': changepoint_prior,
                               'seasonality_prior_scale': seasonality_prior}

    return best_params


def genera_previsione(vendite_storiche, anagrafica, murale, oggi):
    chiavi = []
    descrizioni = []
    previsioni = []
    # Filtra per il murale specificato
    Murale = anagrafica[anagrafica["Murale"] == str(murale)]
    for key in Murale["Key"][:10]:
        vendite_prodotto = vendite_storiche[
            vendite_storiche["Key"] == key].reset_index(drop=True)
        descrizione_prodotto = vendite_prodotto["Descrizione"].iloc[0]

        st.write(vendite_prodotto)
        st.write(descrizione_prodotto)

        vendite_prodotto["Data"] = pd.to_datetime(vendite_prodotto["Data"])
        # Crea un intervallo di date completo fino ad oggi
        date_range = pd.date_range(start='2024-09-01', end=oggi)
        full_dates = pd.DataFrame({'ds': date_range})
        merged_df = pd.merge(full_dates, vendite_prodotto, left_on='ds', right_on='Data', how='left').drop(
            columns=['Data'])
        merged_df['y'] = merged_df['Quantità'].fillna(0)
        merged_df['Key'] = key  # Assegna la chiave del prodotto
        merged_df = merged_df[['ds', 'y', 'Key']]  # Riordina le colonne
        threshold = merged_df['y'].quantile(0.9)
        merged_df['y'] = merged_df['y'].where(merged_df['y'] <= threshold, 0.9)
        threshold = merged_df['y'].quantile(0.9)
        merged_df['y'] = merged_df['y'].apply(lambda x: 0.9 if x > threshold else x)
        previsione = sum(merged_df['y'].rolling(window=5).mean().tail(5))
        #st.write(previsione)
        if previsione > 0:
            previsioni.append(previsione)
        else:
            previsioni.append(0)

        chiavi.append(key)
        descrizioni.append(descrizione_prodotto)

    Dataframe_previsioni = pd.DataFrame()
    Dataframe_previsioni["Key"] = chiavi
    Dataframe_previsioni["Descrizione"] = chiavi
    Dataframe_previsioni["Previsione"] = previsioni

    return Dataframe_previsioni

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

def load_inventario(murale):
    negozio = "todis-viacastelporziano294"
    files_inventario = list_directory_contents(negozio, f"{murale}/Inventari")
    file_piu_recente_inventario = max(files_inventario, key=trova_data_file)
    data_piu_recente_inventario = trova_data_file(file_piu_recente_inventario)
    #data_piu_recente_inventario = datetime.strptime(data_piu_recente_inventario, "%d/%m/%Y")

    try:
        ultimo_inventario = read_csv_from_s3(negozio,f"{st.session_state['murale']}/Inventari",file_piu_recente_inventario,",")
        ultimo_inventario = ultimo_inventario.drop_duplicates(subset="Key")
        ultimo_inventario["Stock"] = ultimo_inventario["Stock"].fillna(0) #QUESTO CI DEVE GIA ESSERE
        ultimo_inventario["Stock"] = ultimo_inventario["Stock"].clip(lower=0)

        return ultimo_inventario
    except:
        ultimo_inventario = read_csv_from_s3(negozio,f"{st.session_state['murale']}/Inventari",file_piu_recente_inventario,";")
        ultimo_inventario = ultimo_inventario.drop_duplicates(subset="Key")
        ultimo_inventario['Stock'] = ultimo_inventario['Stock'].str.replace(',', '.').astype(float)
        ultimo_inventario["Stock"] = ultimo_inventario["Stock"].fillna(0) #QUESTO CI DEVE GIA ESSERE
        ultimo_inventario["Stock"] = ultimo_inventario["Stock"].clip(lower=0)
        return ultimo_inventario


def get_empty_acquisti():
    acquisti = pd.DataFrame(columns=["Key", "Quantità"])
    acquisti["Key"] = 0
    acquisti["Quantità"] = 0
    return acquisti

# Funzione per creare card
def mostra_promozioni(promozioni, tipo):
    if promozioni.empty:
        st.info(f"Nessuna promozione {tipo}.")
        return

    promozioni = promozioni.reset_index(drop=True)
    for i in range(0, len(promozioni), 3):
        cols = st.columns(3)  # 3 colonne per riga
        for j, col in enumerate(cols):
            if i + j < len(promozioni):
                row = promozioni.iloc[i + j]
                col.markdown(
                    f"""
                    <div class="promo-card {tipo}">
                        <h4>{row['Descrizione']}</h4>
                        <p>Dal: {row['Data_inizio'].strftime('%d %b %Y')} al {row['Data_fine'].strftime('%d %b %Y')}</p>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )


def master_job_aggiornamento():
    murali = ["murale-300"]

    for m in murali:
        st.session_state['murale'] = m

        # Titolo dell'app
        negozio = "todis-viacastelporziano294"

        files_vendite = list_directory_contents(negozio, f"{st.session_state['murale']}/Storico_Vendite")
        files_vendite_giornaliero = list_directory_contents(negozio,
                                                            f"{st.session_state['murale']}/Vendite_giornaliere_pulite")
        files_acquisti_giornaliero = list_directory_contents(negozio,
                                                             f"{st.session_state['murale']}/Acquisti_giornalieri_puliti")

        file_piu_recente_storico = max(files_vendite, key=trova_data_file)
        data_piu_recente_storico = trova_data_file(file_piu_recente_storico)
        data_piu_recente_storico_datetime = data_piu_recente_storico.date()



        Vendite_storiche = read_csv_from_s3(negozio, f"{st.session_state['murale']}/Storico_Vendite",
                                            file_piu_recente_storico, ",")
        try:
            if not Vendite_storiche:
                Vendite_storiche = read_csv_from_s3(negozio, f"{st.session_state['murale']}/Storico_Vendite",
                                                    file_piu_recente_storico, ";")
        except:
            pass


        with st.spinner(text="Aggiornando file di Vendita Storica!"):
            # Aggiorna vendite storiche
            for el in files_vendite_giornaliero:
                data = el.split('_')[1] + '/' + el.split('_')[2] + '/' + el.split('_')[3].replace('.csv', '')
                data = datetime.strptime(data, "%d/%m/%Y")

                if data.date() > data_piu_recente_storico_datetime:
                    Vendite_giornaliere = read_csv_from_s3(negozio,
                                                           f"{st.session_state['murale']}/Vendite_giornaliere_pulite",
                                                           el, ",")
                    Vendite_storiche = pd.concat([Vendite_storiche, Vendite_giornaliere], ignore_index=True)
                    #st.write(Vendite_giornaliere)

                    upload_dataframe_as_csv(
                        Vendite_storiche,
                        negozio,
                        f"{st.session_state['murale']}/Storico_Vendite",
                        f"Storico_Vendite_{el}"
                    )

        files_inventario = list_directory_contents(negozio, f"{st.session_state['murale']}/Inventari")
        file_piu_recente_inventario = max(files_inventario, key=trova_data_file)
        data_piu_recente_inventario = trova_data_file(file_piu_recente_inventario)
        data_piu_recente_inventario = data_piu_recente_inventario.date()
        ultimo_inventario = load_inventario(st.session_state["murale"])

        with st.spinner(text="Aggiornando file di inventario!"):
            while True:
                ultimo_inventario.loc[ultimo_inventario["Stock"] < 0, "Stock"] = 0
                data_prossima = data_piu_recente_inventario + timedelta(days=1)
                data_prossima_str = data_prossima.strftime("%d_%m_%Y")
                data_vendite_str = data_piu_recente_inventario.strftime("%d_%m_%Y")

                file_acquisti = f"Acquisti_{data_prossima_str}.csv"
                file_vendite = f"Vendite_{data_vendite_str}.csv"

                if pd.to_datetime(data_prossima_str.replace("_","/"),format="%d/%m/%Y").weekday() != 6:
                    if not file_exists_in_s3(negozio,
                                             f"{st.session_state['murale']}/Acquisti_giornalieri_puliti/{file_acquisti}"):
                        print(f"File acquisti non trovato per il {data_prossima_str}. Interruzione aggiornamento.")
                        break
                    acquisti = read_csv_from_s3(negozio, f"{st.session_state['murale']}/Acquisti_giornalieri_puliti",
                                                file_acquisti, ",")

                else:
                    acquisti = get_empty_acquisti()


                if not file_exists_in_s3(negozio,
                                         f"{st.session_state['murale']}/Vendite_giornaliere_pulite/{file_vendite}"):
                    print(f"File vendite non trovato per il {data_vendite_str}. Interruzione aggiornamento.")
                    break

                vendite = read_csv_from_s3(negozio, f"{st.session_state['murale']}/Vendite_giornaliere_pulite",
                                           file_vendite, ",")

                acquisti = acquisti[["Key","Quantità"]].groupby("Key").sum().reset_index()
                vendite = vendite[["Key", "Quantità"]].groupby("Key").sum().reset_index()

                inventario_aggiornato = ultimo_inventario.copy()
                #st.write(inventario_aggiornato)
                inventario_aggiornato = inventario_aggiornato.merge(vendite[["Key", "Quantità"]], on="Key", how="left",
                                                                    suffixes=('', '')).fillna(0)
                inventario_aggiornato = inventario_aggiornato.merge(acquisti[["Key", "Quantità"]], on="Key", how="left",
                                                                    suffixes=('', '_acquisti')).fillna(0)

                #st.write("Inventario prima " + str(inventario_aggiornato["Stock"].sum()))
                #st.write("Importo acquisti " + str(acquisti["Quantità"].sum()))
                #st.write("Importo vendite " + str(vendite["Quantità"].sum()))


                inventario_aggiornato["Stock"] = (
                        inventario_aggiornato["Stock"] - inventario_aggiornato["Quantità"] + inventario_aggiornato[
                    "Quantità_acquisti"]
                )
                #st.write(data_prossima_str)
                #st.write("Inventario dopo " + str(inventario_aggiornato["Stock"].sum()))

                inventario_aggiornato = inventario_aggiornato.drop(["Quantità", "Quantità_acquisti"], axis=1)

                upload_dataframe_as_csv(
                    inventario_aggiornato,
                    negozio,
                    f"{st.session_state['murale']}/Inventari",
                    f"Inventario_{data_prossima_str}.csv"
                )

                ultimo_inventario = inventario_aggiornato
                data_piu_recente_inventario = data_prossima


def terminalino(ordine):
    info_terminalino = pd.read_excel(
        "files_utili/cosi infernetto.xls")

    ordine = ordine[ordine["pacchi_da_ordinare"] != 0]
    info_terminalino = info_terminalino.dropna(subset="cod. term.")

    # Estrai `cod. articolo`, `variante`, e `quantità` dal DataFrame ordine
    ordine['cod. articolo'] = ordine['Key'].apply(lambda x: x.split('-')[0].zfill(6))
    ordine['variante'] = ordine['Key'].apply(lambda x: x.split('-')[1].zfill(2))
    ordine['quantità'] = ordine['pacchi_da_ordinare'].apply(lambda x: str(x).zfill(5) + "00")

    data_oggi = datetime.now().strftime('%d%m%Y').zfill(8)
    ora_adesso = datetime.now().strftime('%H%M').zfill(4)

    # Creazione del DataFrame finale, rinominato in "ordine_automatico"
    ordine_automatico = pd.DataFrame({
        'cod. term.': ['0H03'] * len(ordine),
        'cod. promoter': ["000000"] * len(ordine),
        'cod. p.vend.': ["101174"] * len(ordine),
        'progressivo scarico': ["066"] * len(ordine),
        'data ordine': [str(data_oggi)] * len(ordine),
        'ora ordine': [str(ora_adesso)] * len(ordine),
        'data consegna': ["00000000"] * len(ordine),  # Personalizza se necessario
        'magazzino riordino': ["82"] * len(ordine),
        'corsia': [None] * len(ordine),  # Personalizza se necessario
        'listino': ["79"] * len(ordine),
        'n°listino': ["000000"] * len(ordine),
        'tipo ean': ["79"] * len(ordine),
        'cod. articolo': ordine['cod. articolo'].astype(str),
        'variante': ordine['variante'].astype(str),
        'quantità': ordine['quantità']
    })

    ordine_automatico['formattato'] = (
            ordine_automatico['cod. term.'].astype(str) +
            ordine_automatico['cod. promoter'].astype(str) +
            ordine_automatico['cod. p.vend.'].astype(str) +
            ordine_automatico['progressivo scarico'].astype(str) +
            ordine_automatico['data ordine'].astype(str) +
            ordine_automatico['ora ordine'].astype(str) +
            ordine_automatico['data consegna'].astype(str) +
            ordine_automatico['magazzino riordino'].astype(str) +
            "  " +
            ordine_automatico['listino'].astype(str) +
            ordine_automatico['n°listino'].astype(str) +
            ordine_automatico['tipo ean'].astype(str) +
            ordine_automatico['cod. articolo'].astype(str) +
            ordine_automatico['variante'].astype(str) +
            ordine_automatico['quantità'].astype(str)
    )

    return ordine_automatico

def create_excel_file(df):
    output = BytesIO()  # Create an in-memory binary stream
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Ordine")
    output.seek(0)  # Reset the pointer to the beginning of the stream
    return output

