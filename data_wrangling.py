import pandas as pd
import duckdb
import os

# Get connection parameters
motherduck_dsn = os.getenv("motherduck_dsn")

# Connect to motherduck data warehouse
con = duckdb.connect(motherduck_dsn)

# 