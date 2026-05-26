"""
streaming_query.py

Opens a streaming cursor against Confluent Cloud Flink SQL and continuously
prints high-value orders as they arrive in the materialized table.

Press Ctrl+C to stop.

Usage:
    cp .env.example .env   # fill in your credentials
    python python/streaming_query.py
"""

import os
import time
from datetime import datetime
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
print("Streaming high-value orders from high_value_orders... (Ctrl+C to stop)\n")
print(f"{'order_id':>10} {'product_name':<35} {'amount':>10} {'event_time':>23}")
print("-" * 84)

cursor = conn.streaming_cursor()
cursor.execute(
    "SELECT order_id, product_name, amount, event_time FROM high_value_orders"
)

try:
    while cursor.may_have_results:
        rows = cursor.fetchmany(10)
        for row in rows:
            order_id, product_name, amount, event_time = row
            ts = datetime.now().strftime("%H:%M:%S")
            print(
                f"[{ts}] "
                f"{int(order_id):>10} "
                f"{str(product_name):<35} "
                f"${float(amount):>9,.2f} "
                f"{str(event_time):>23}"
            )
        if not rows:
            time.sleep(0.1)
except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    cursor.close()
    conn.close()
