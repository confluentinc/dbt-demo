"""
snapshot_query.py

Runs a point-in-time (snapshot) query against Confluent Cloud Flink SQL
and loads the results into a pandas DataFrame.

Usage:
    cp .env.example .env   # fill in your credentials
    python python/snapshot_query.py
"""

import os
import pandas as pd
from dotenv import load_dotenv
import confluent_sql

load_dotenv()

REQUIRED = [
    "CONFLUENT_ORG_ID",
    "CONFLUENT_ENV_ID",
    "CONFLUENT_CLOUD_PROVIDER",
    "CONFLUENT_CLOUD_REGION",
    "CONFLUENT_FLINK_API_KEY",
    "CONFLUENT_FLINK_API_SECRET",
    "CONFLUENT_COMPUTE_POOL_ID",
    "CONFLUENT_DBNAME",
]

missing = [v for v in REQUIRED if not os.getenv(v)]
if missing:
    raise SystemExit(f"Missing environment variables: {', '.join(missing)}\nSee .env.example")

conn = confluent_sql.connect(
    organization_id=os.environ["CONFLUENT_ORG_ID"],
    environment_id=os.environ["CONFLUENT_ENV_ID"],
    cloud_provider=os.environ["CONFLUENT_CLOUD_PROVIDER"],
    cloud_region=os.environ["CONFLUENT_CLOUD_REGION"],
    flink_api_key=os.environ["CONFLUENT_FLINK_API_KEY"],
    flink_api_secret=os.environ["CONFLUENT_FLINK_API_SECRET"],
    compute_pool_id=os.environ["CONFLUENT_COMPUTE_POOL_ID"],
    database=os.environ["CONFLUENT_DBNAME"],
)

print("Connected to Confluent Cloud Flink.")
print("Running snapshot query on stg_orders...\n")

df = pd.read_sql("SELECT * FROM stg_orders LIMIT 20", conn)
conn.close()

print(df.to_string(index=False))
print(f"\n{len(df)} rows returned.")
