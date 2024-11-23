import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.service_account import Credentials
import pandas as pd
import duckdb
import io
from datetime import datetime

# Get credentials and settings from environment variables
SERVICE_ACCOUNT_FILE = 'service_account.json'
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SHARED_FOLDER_ID = os.getenv('SHARED_FOLDER_ID')
MOTHERDUCK_DSN = os.getenv('MOTHERDUCK_DSN')

# Authenticate to Google Drive API
credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=credentials)

def list_files_in_drive_folder(folder_id):
    results = drive_service.files().list(
        q=f"'{folder_id}' in parents",
        fields="files(id, name)"
    ).execute()
    return results.get('files', [])

def download_file(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh

def process_and_load(file_stream, table_name):
    df = pd.read_excel(file_stream)
    con = duckdb.connect(database=MOTHERDUCK_DSN)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
    print(f"Data from {table_name} loaded successfully to MotherDuck.")
    con.close()

def etl_process():
    print(f"Starting ETL process at {datetime.now()}...")
    files = list_files_in_drive_folder(SHARED_FOLDER_ID)

    for file in files:
        if file['name'].endswith('.xlsx'):
            print(f"Processing file: {file['name']}")
            file_stream = download_file(file['id'])
            process_and_load(file_stream, table_name='excel_data')

if __name__ == "__main__":
    etl_process()
