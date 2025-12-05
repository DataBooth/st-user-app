import duckdb
import os
from dotenv import load_dotenv


load_dotenv()

# Ensure MOTHERDUCK_TOKEN is set
token = os.getenv("MOTHERDUCK_TOKEN")
if not token:
    raise ValueError("MotherDuck token is missing!")

# Test connection
conn = duckdb.connect(f"md:dementia_data?motherduck_token={token}")
result = conn.execute("SHOW DATABASES;").fetchdf()
print(result)
conn.close()
