import boto3
import pandas as pd
import os
import io
from dotenv import load_dotenv
import streamlit as st


load_dotenv()
def initialize_s3():
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')  # Load the region from the .env file
    )
    return s3

'''
def initialize_s3():
    s3 = boto3.client(
        's3',
        aws_access_key_id = st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key = st.secrets["AWS_SECRET_ACCESS_KEY"],
        region_name = st.secrets["AWS_REGION"]
    )
    return s3
'''

def read_csv_from_s3(bucket_name, directory, file_name,delimiter):
    s3 = initialize_s3()
    if s3 is None:
        print("Failed to initialize S3 client.")
        return None

    file_path = f"{directory}/{file_name}" if directory else file_name
    try:
        print(f"Attempting to retrieve file from S3: bucket={bucket_name}, key={file_path}")
        response = s3.get_object(Bucket=bucket_name, Key=file_path)

        # Check if the response is valid before proceeding
        if response is None or 'Body' not in response:
            print("Failed to retrieve file or response is missing 'Body'.")
            return None

        print("File retrieved successfully, now loading into pandas.")

        # Attempt to read the CSV content using pandas
        csv_data = pd.read_csv(response['Body'],delimiter=delimiter,encoding='latin-1')
        if "Unnamed: 0" in csv_data.columns:
            csv_data = csv_data.drop(["Unnamed: 0"],axis=1)

        print("CSV loaded successfully.")
        return csv_data

    except Exception as e:
        print(f"Error reading CSV file from S3: {e}")
        return None

def upload_dataframe_as_csv(dataframe, bucket_name, directory, file_name):
    s3 = initialize_s3()
    s3_key = f"{directory}/{file_name}" if directory else file_name

    # Usa un buffer in memoria per evitare di salvare su disco
    csv_buffer = io.StringIO()
    dataframe.to_csv(csv_buffer, index=False,sep=",")  # Salva il DataFrame nel buffer come CSV
    try:
        # Carica il contenuto del buffer su S3
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
        print(f"DataFrame caricato con successo come '{s3_key}' in '{bucket_name}'")
    except Exception as e:
        print(f"Errore durante l'upload del DataFrame: {e}")


def create_directory(bucket_name, directory):
    s3 = initialize_s3()
    # Aggiungi "/" alla fine del nome della directory se non c'è già
    dir_path = directory if directory.endswith('/') else f"{directory}/"
    try:
        s3.put_object(Bucket=bucket_name, Key=dir_path)
        print(f"Directory '{dir_path}' creata con successo.")
    except Exception as e:
        print(f"Errore nella creazione della directory: {e}")

def list_directory_contents(bucket_name, directory=""):
    s3 = initialize_s3()
    # Imposta il prefisso solo se directory non è vuota
    dir_path = f"{directory}/" if directory else ""

    try:
        # Usa Delimiter per ottenere solo le "cartelle" di primo livello se directory è vuota
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=dir_path, Delimiter='/')

        # Se directory è vuota, ottieni solo i nomi delle cartelle principali
        if directory == "" and 'CommonPrefixes' in response:
            folder_names = [prefix['Prefix'].rstrip('/') for prefix in response['CommonPrefixes']]
            return folder_names
        # Se directory non è vuota, ottieni gli oggetti all'interno
        elif 'Contents' in response:
            file_names = [item['Key'] for item in response['Contents']]
            file_names = [name[len(dir_path):] for name in file_names if name != dir_path]
            return file_names
        else:
            print(f"La directory '{directory}' è vuota o non esiste.")
            return []
    except Exception as e:
        print(f"Errore nel recuperare i contenuti della directory: {e}")
        return []


def file_exists_in_s3(bucket_name, file_path):
    """
    Verifica se un file esiste in una directory S3.

    :param bucket_name: Nome del bucket S3.
    :param file_path: Percorso completo del file in S3 (directory + nome file).
    :return: True se il file esiste, False altrimenti.
    """
    s3_client = initialize_s3()

    try:
        # Usa il metodo head_object per verificare l'esistenza del file
        s3_client.head_object(Bucket=bucket_name, Key=file_path)
        return True
    except s3_client.exceptions.ClientError as e:
        # Se il codice di errore è 404, il file non esiste
        if e.response['Error']['Code'] == "404":
            return False
        else:
            # Rilancia l'errore se è un altro tipo di eccezione
            raise e
