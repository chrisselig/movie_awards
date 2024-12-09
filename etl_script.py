import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.service_account import Credentials
import pandas as pd
import json
import io
import duckdb
import openpyxl
from datetime import datetime
import re
from utils import list_files_in_drive_folder, download_file, process_excel_file, load_to_motherduck

# Variables
download_directory = "data"

# Load configuration
folder_id = os.getenv("folder_id")  # Try to get from environment variables
motherduck_dsn = os.getenv("motherduck_dsn")  # Try to get from environment variables
config_file = "config.json"  # Path to local configuration file

if not folder_id or not motherduck_dsn:
    # If not provided via environment variables, load from config.json
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
            folder_id = folder_id or config.get("folder_id")
            motherduck_dsn = motherduck_dsn or config.get("motherduck_dsn")
            if not folder_id or not motherduck_dsn:
                raise ValueError("Both 'folder_id' and 'motherduck_dsn' must be set in the config file.")
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file '{config_file}' not found.")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding JSON from '{config_file}': {e}")

# Path to your service account JSON file
SERVICE_ACCOUNT_FILE = 'service_account.json'

# Authenticate with Google APIs
credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)

# Build the Drive API client
drive_service = build('drive', 'v3', credentials=credentials)


def etl_process():
    """Main ETL process to handle downloading, processing, and loading data."""
    print(f"Starting ETL process at {datetime.now()}...")
    files = list_files_in_drive_folder(folder_id, drive_service)  # Pass drive_service here

    if not files:
        print("No files found in the shared folder.")
        return

    for file in files:
        file_name = file['name']
        file_stream = download_file(file['id'], file_name=file['name'], download_dir=download_directory, drive_service=drive_service)
        
        if file_stream:
            # Sanitize the file name to create a valid table name, and ensure it starts with 'stg_'
            sanitized_table_name = "stg_" + re.sub(r'\W+', '_', file_name.replace('.xlsx', '').strip().lower())
            print(f"Processing file: {file_name}, creating table: {sanitized_table_name}")
            
            # Process the Excel file into a DataFrame
            df = process_excel_file(file_stream)
            if df is not None:
                # Load the DataFrame into MotherDuck
                load_to_motherduck(df, sanitized_table_name, motherduck_dsn)




if __name__ == "__main__":
    etl_process()
