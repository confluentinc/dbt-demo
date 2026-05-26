# Manage your streaming pipeline lifecycle with dbt and materialized tables

This demo repo shows how you can build streaming pipelines with **materialized tables** on **Confluent Cloud Flink** using **dbt-confluent**. In this demo, you will use dbt to deploy three Confluent Cloud Flink SQL statements related to online order processing: 

- A **streaming_source** that generates sample orders data in **raw_orders**. 
- A **view** that removes invalid orders for staging in **stg_orders**. 
- A custom [materialization](https://docs.getdbt.com/guides/create-new-materializations?step=1), **materialized_table**, that captures orders of a high value in the **high_value_orders** topic. 

**Data flow:**

```
raw_orders (streaming_source, Faker)
  └─ stg_orders (view — cleans status, filters amount > 0)
       └─ high_value_orders (materialized_table — orders >= threshold, refreshed every minute)
```

## Video Walkthrough

[![Video walkthrough of this demo](https://img.youtube.com/vi/HgxcAdZrvIY/maxresdefault.jpg)](https://www.youtube.com/watch?v=HgxcAdZrvIY)

## Why dbt and materialized tables on Flink

Data engineers have standardized on dbt for SQL-based transformation because it brings version control, testing, documentation, and CI/CD to the data warehouse. Until now, bringing those same practices to Flink SQL meant ad-hoc scripts or copy-pasting code into a console. The **dbt Adapter for Confluent Cloud for Apache Flink** closes that gap: teams can define streaming pipelines as dbt models, test them with mock data, generate documentation, and deploy them to Flink compute pools using the same dbt workflow they already use for batch. Existing project patterns and skills carry over directly, so teams migrating real-time workloads to Confluent can go live in days rather than months. A companion `confluent-sql` driver further opens Flink to the broader Python ecosystem — pandas, Airflow, and AI frameworks plug in with no special tooling.

**Materialized tables** turn Flink statements into persistent, database-like objects whose lifecycle is managed in SQL. Historically, even a small change like adding a column required a stop-and-recreate process with manual offset management to prevent data loss — a real risk every time a pipeline evolved. With `CREATE OR ALTER MATERIALIZED TABLE`, you evolve the query in place and Flink orchestrates the complex mechanics of offset bookkeeping and job migration under the hood. That marks the end of manual migration cycles and makes operating Flink pipelines in production feel like editing a SQL view: schema and logic changes become everyday edits rather than maintenance events. The `high_value_orders` model in this demo uses this exact pattern.

## Prerequisites

You must have:

- A **Confluent Cloud** account
- A Confluent Cloud **Cloud API key** with `OrganizationAdmin` — used by Terraform to provision resources. This is *different* from the Flink region key the demo consumes at runtime; Terraform creates that one for you.
- **Terraform** `>= 1.5`
- Python 3.10+

The Confluent Cloud environment, Kafka cluster, Flink compute pool, service account, and Flink region API key are all created by the Terraform module in `terraform/`. See [`terraform/README.md`](terraform/README.md) for the full resource list and customization options.

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

## Provision Confluent Cloud infrastructure

1. Export your Cloud API key/secret so the Terraform provider can authenticate

    ```bash
    export CONFLUENT_CLOUD_API_KEY=...
    export CONFLUENT_CLOUD_API_SECRET=...
    ```

2. Apply the module

    ```bash
    cd terraform
    terraform init
    terraform apply        # 5-10 min, mostly Kafka cluster provisioning
    cd ..
    ```

    This stands up a Standard environment, a Standard Kafka cluster, a Flink compute pool, a service account with `FlinkDeveloper`, and a Flink region API key. Defaults to AWS `us-east-2`; pass `-var region=...` to apply elsewhere.

## Configure

1. Generate your `.env` directly from the Terraform outputs

    ```bash
    terraform -chdir=terraform output -raw env_file > .env
    ```

2. Load env vars into your shell

    ```bash
    source .env
    ```

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

**dbt-confluent** uses the [confluent-sql Python library](https://github.com/confluentinc/confluent-sql)  under the hood, and you can also use it directly in your Python code. In this section, you will explore and run two sample queries on your data directly from Python using **confluent-sql**. 

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

## Cleanup

To tear down everything Terraform provisioned (environment, cluster, compute pool, service account, Flink API key), run:

```bash
cd terraform
terraform destroy
```

If instead you'd like to keep the environment and only drop the demo's models, navigate to the SQL workspace for your Flink compute pool and run:

```sql
DROP TABLE raw_orders;
```

```sql
DROP VIEW stg_orders;
```

```sql
DROP MATERIALIZED TABLE high_value_orders;
```