# Manage your streaming pipeline lifecycle with dbt and materialized tables

This demo repo shows how you can build streaming pipelines with **materialized tables** on **Confluent Cloud Flink** using **dbt-confluent**. In this demo, you will use dbt to deploy three Confluent Cloud Flink SQL statements related to online order processing: 

- A **streaming_source** that generates sample orders data in **raw_orders**. 
- A **view** that removes invalid orders for staging in **stg_orders**. 
- A **materialized_table** that shows captures orders of a high value in the **high_value_orders** topic. 

**Data flow:**

```
raw_orders (streaming_source, Faker)
  └─ stg_orders (view — cleans status, filters amount > 0)
       └─ high_value_orders (materialized_table — orders >= threshold, refreshed every minute)
```

## Prerequisites

You must have:

- A **Confluent Cloud** account with a Flink-enabled environment
- An active **Flink compute pool** (`RUNNING` state) — `lfcp-` prefixed ID
- An existing **Kafka cluster** within that environment (this is the Flink "database")
- **Flink Region API credentials** — create these under Flink > Compute Pools > API Keys
  (these are *different* from control-plane API keys)
- Python 3.10+

This repo does not create Confluent Cloud infrastructure. All resources must exist before running.

## Install

1. Clone the repo

    ```bash
    git clone <repo-url>
    cd dbt-demo
    ```

2. Install Python dependencies (including dbt-confluent)
  
    ```bash
    pip install -r requirements.txt
    ```

3. Confirm the Confluent adapter is registered
  
    ```bash
    dbt --version
    ```
  
    You should see "confluent" listed under Plugins

4. Install dbt packages (if any are added later)
  
    ```bash
    dbt deps
    ```

## Configure

1. Create your `.env`

    ```bash
    cp .env.example .env
    ```

    Open `.env`, fill in all values, and save the file. See `.env.example` for field descriptions.

2. Load env vars into your shell

    ```bash
    source .env
    ```

    The dbt profile lives at `.dbt/profiles.yml` in this repo and reads all credentials from environment variables, so no secrets live in the file itself. `.env.example` sets `DBT_PROFILES_DIR` to point dbt at it — no copy into `~/.dbt` is needed.

## Run dbt

1. Create all models in Confluent Cloud Flink

    ```bash
    dbt run
    ```

2. Run data tests and unit tests to verify the quality, reliability, and accuracy of your models

    ```bash
    dbt test
    ```

3. Generate and serve documentation for your models. The adapter pulls schema metadata from Flink's INFORMATION_SCHEMA, so the docs site reflects live streaming pipeline structure.

    ```bash
    dbt docs generate
    dbt docs serve
    ```

    Open http://localhost:8080 in your browser to view the documentation. 

## View the resources in Confluent

After `dbt run` completes, open the **Confluent Cloud console → Flink → Statements** to see the streaming source and streaming table running in real time. You can also visit **Clusters → <your_cluster> → Topics → high_value_orders** to see messages populating in the materialized table. 

## Iterating on logic

Materialized tables on Confluent allow you to change your SQL logic, re-run dbt, and have Flink continue to manage the refresh pipeline in place. Next, you will see this in action by changing the value threshold for your **high_value_orders topic** from $300 to $200. 

1. Open `models/marts/high_value_orders.sql` and lower the threshold:

    ```diff
    - WHERE amount >= 300
    + WHERE amount >= 200
    ```

2. Apply the change:

    ```bash
    dbt run -s high_value_orders
    ```

3. Open the **Confluent Cloud console → Flink → Statements** and view the **History** tab to see the updated SQL logic. Then, navigate to **Clusters → <your_cluster> → Topics → high_value_orders** and view some messages to verify that your value threshold has changed. 

## Run the Python scripts

**dbt-confluent** uses the [confluent-sql](https://github.com/confluentinc/confluent-sql) Python library under the hood, and you can also use it directly in your Python code. In this section, you will explore and run two sample queries on your data directly from Python using **confluent-sql**. 

### Snapshot query

```bash
python python/snapshot_query.py
```

This script uses `pd.read_sql(sql, conn)` — because confluent_sql is DB-API v2 compliant, it plugs directly into pandas, as well as tools like Airflow, Dagster, Streamlit, LangChain, Hex, and Metabase.

Expected output:

```
Connected to Confluent Cloud Flink.
Running snapshot query on stg_orders...

 order_id  customer_id  product_id  product_name          status     amount  event_time
   482103          317          12  Rustic Cotton Shirt    placed    123.45   2026-05-06 09:14:20
   719204           88          31  Sleek Wooden Table     shipped   287.00   2026-05-06 09:14:21
...

20 rows returned.
```

### Streaming query

You can also run streaming queries that continuously print live updates:  

```bash
python python/streaming_query.py
# Press Ctrl+C to stop
```

Expected output:

```
Connected to Confluent Cloud Flink.
Streaming high-value orders from high_value_orders... (Ctrl+C to stop)

  order_id product_name                            amount              event_time
------------------------------------------------------------------------------------
[09:14:22]     482103 Rustic Cotton Shirt                $   412.50 2026-05-06 09:14:20
[09:14:23]     719204 Sleek Wooden Table                 $   387.00 2026-05-06 09:14:22
[09:14:24]     482877 Premium Walnut Desk                $   478.25 2026-05-06 09:14:23
...
```

Each line is a high-value order (amount ≥ threshold) emitted by the materialized table as new orders arrive.