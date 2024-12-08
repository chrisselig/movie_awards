import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.service_account import Credentials
import pandas as pd
import json
import duckdb
import io
import openpyxl
from datetime import datetime
import re

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


def list_files_in_drive_folder(folder_id):
    """List .xlsx files in a specified Google Drive folder."""
    try:
        results = drive_service.files().list(
            q=f"'{folder_id}' in parents",
            fields="files(id, name)"
        ).execute()
        files = results.get('files', [])
        # Filter files to include only .xlsx files and exclude those with '~' in the name
        xlsx_files = [file for file in files if 'name' in file and file['name'].endswith('.xlsx') and '~' not in file['name']]
        return xlsx_files
    except Exception as e:
        print(f"Error listing files in folder {folder_id}: {e}")
        return []



def download_file(file_id, file_name, download_dir):
    """Download a file from Google Drive by file ID and save it to a specified directory."""
    try:
        # Ensure the download directory exists
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        
        # Create a request to download the file
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()  # A file-like object to store the file content
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)  # Reset the file pointer to the beginning
        
        # Save the file to the specified directory
        file_path = os.path.join(download_dir, file_name)
        with open(file_path, "wb") as f:
            f.write(fh.read())
        
        print(f"Downloaded {file_name} successfully to {file_path}.")
        return file_path  # Return the full file path
    except Exception as e:
        print(f"Error downloading file '{file_name}' (ID: {file_id}): {e}")
        return None


def process_and_load(file_stream, table_name):
    """Process an Excel file and load its contents into a DuckDB table."""
    try:
        # Load data into a Pandas DataFrame
        df = pd.read_excel(file_stream)

        # Standardize column names (optional: handle renaming or cleaning)
        df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]

        # Standardize column names: strip whitespace, remove single quotes, and replace special characters with an underscore
        df.columns = [
            re.sub(r'\W+', '_', col.strip().replace("'", "")) for col in df.columns
        ]

        # Connect to DuckDB (or MotherDuck using the DSN)
        con = duckdb.connect(database=motherduck_dsn)

        # Drop and recreate the table to handle schema changes
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df LIMIT 0")

        # Insert data into the recreated table
        con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        print(f"Data from {table_name} loaded successfully into MotherDuck.")

        # Close the connection
        con.close()

    except Exception as e:
        print(f"Error processing and loading file into {table_name}: {e}")


def etl_process():
    """Main ETL process to handle downloading, processing, and loading data."""
    print(f"Starting ETL process at {datetime.now()}...")
    files = list_files_in_drive_folder(folder_id)

    if not files:
        print("No files found in the shared folder.")
        return

    for file in files:
        file_name = file['name']
        file_stream = download_file(file['id'], file_name=file['name'], download_dir=download_directory)
        
        if file_stream:
            # Sanitize the file name to create a valid table name, and ensure it starts with 'stg_'
            sanitized_table_name = "stg_" + re.sub(r'\W+', '_', file_name.replace('.xlsx', '').strip().lower())
            print(f"Processing file: {file_name}, creating table: {sanitized_table_name}")
            
            # Process and load the file
            process_and_load(file_stream, sanitized_table_name)


if __name__ == "__main__":
    etl_process()
