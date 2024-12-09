# utils.py
import pandas as pd
import duckdb
import re
import os
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.service_account import Credentials


def list_files_in_drive_folder(folder_id, drive_service):
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



def download_file(file_id, file_name, download_dir, drive_service):
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

def process_excel_file(file_stream):
    """Process an Excel file into a Pandas DataFrame with standardized column names."""
    try:
        # Load data into a Pandas DataFrame
        df = pd.read_excel(file_stream)

        # Standardize column names: strip whitespace, remove single quotes, replace special characters with underscores
        df.columns = [
            re.sub(r'\W+', '_', col.strip().replace("'", "")).lower() for col in df.columns
        ]

        print("Excel file processed successfully.")
        return df

    except Exception as e:
        print(f"Error processing Excel file: {e}")
        return None

def load_to_motherduck(df, table_name, motherduck_dsn):
    """Load a Pandas DataFrame into a DuckDB table in MotherDuck."""
    try:
        # Connect to DuckDB (or MotherDuck)
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
        print(f"Error loading data into MotherDuck: {e}")