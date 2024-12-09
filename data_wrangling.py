# %% 

import pandas as pd
import duckdb
import os
import json
from utils import load_to_motherduck

# Get connection parameters
motherduck_dsn = os.getenv("motherduck_dsn")

config_file = "config.json"  # Path to local configuration file

if not motherduck_dsn:
    # If not provided via environment variables, load from config.json
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
            motherduck_dsn = motherduck_dsn or config.get("motherduck_dsn")
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file '{config_file}' not found.")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding JSON from '{config_file}': {e}")

# Connect to motherduck data warehouse
con = duckdb.connect(database=motherduck_dsn)

# Extract stage data ----
try:
    stg_best_movies = con.sql(
        r"""
        SELECT *,
            'Best Movies' as origin_file 
        FROM stg_best_movies
        """
        ).df()
    stg_worst_movies = con.sql(
        r"""
        SELECT *,
            'Worst Movies' as origin_file 
        FROM stg_worst_movies
        """
        ).df()
except Exception as e:
    print(f"Error querying the database: {e}")

# Clean best and worst movies ----
movies = pd.concat([stg_best_movies,stg_worst_movies],ignore_index=True)


# Write back to motherduck ----
load_to_motherduck(table_name = movies, file_stream=movies)