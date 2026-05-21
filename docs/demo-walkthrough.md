# Demo Walkthrough — Confluent + dbt

Presenter script for a 2-minute live demo. Timestamps align with the sections below.

---

## Setup (before the demo starts)

1. Open a terminal in the repo root.
2. Confirm `.env` is populated — never show secrets on screen:
   ```bash
   cat .env | grep -v SECRET | grep -v API_KEY
   ```
3. Confirm `DBT_PROFILES_DIR` points at the repo profile:
   ```bash
   echo "$DBT_PROFILES_DIR" && ls "$DBT_PROFILES_DIR/profiles.yml"
   ```
4. Verify the Flink compute pool is `RUNNING` in the Confluent Cloud console (Flink → Compute Pools).

---

## [0:00–0:20] What is dbt?

```bash
ls -1
cat dbt_project.yml
```

**Say:** "dbt is the standard way data engineers build analytics pipelines with SQL. You define models as SQL files, add tests and documentation, and run everything with `dbt run` and `dbt test` in CI/CD — just like application code. This looks like any other dbt project."

---

## [0:20–0:40] Why dbt + Confluent?

*(No new command — keep the terminal visible.)*

**Say:** "Until now, those dbt models mostly targeted batch warehouses like Snowflake or BigQuery. With Confluent, you can point that same dbt workflow at Apache Flink on Confluent Cloud, so the exact same models become continuous streaming pipelines over Kafka data — no new tooling, no custom scripts, just dbt."

---

## [0:40–1:15] dbt-confluent in action

### Confirm the adapter

```bash
dbt --version
```

**Say:** "I've installed the adapter with `pip install dbt-confluent`. You can see `confluent` listed here under Plugins."

Expected output (relevant lines):
```
Core:
  - installed: 1.x.x
Plugins:
  - confluent: 1.x.x
```

### Show the profile

```bash
cat .dbt/profiles.yml
```

**Say:** "In `profiles.yml`, instead of pointing at a warehouse, I point dbt at Confluent Cloud: cloud provider and region, my Flink compute pool, and an API key and secret. Everything else stays standard dbt."

### Walk the three model types

**View — reads from an existing Kafka-backed table:**

```bash
cat models/staging/stg_orders.sql
```

**Say:** "A view that reads from raw_orders — a Kafka-backed table — and does basic cleaning. This is identical to any dbt view you've written before."

**Materialized table — declarative refresh:**

```bash
cat models/marts/high_value_orders.sql
```

**Say:** "A `materialized_table` — Flink keeps this filter continuously refreshed every minute via `CREATE MATERIALIZED TABLE ... FRESHNESS = INTERVAL '1' MINUTE`. We declare the freshness we need and Flink handles the refresh. The threshold in the `WHERE` clause is just a normal SQL knob — in a minute we'll change it and watch the pipeline update."

**Streaming source — connector-backed data generation:**

```bash
cat models/sources/raw_orders.sql
```

**Say:** "And a `streaming_source` that uses the Faker connector to generate demo data — five synthetic orders per second, no Kafka producer needed."

### Run dbt

```bash
dbt run
```

**Say:** "When I run `dbt run`, the adapter translates each model into Flink SQL and submits it to the compute pool."

Expected output:
```
1 of 3 OK   created streaming_source model raw_orders          [SUCCESS in Xs]
2 of 3 OK   created view model stg_orders                      [SUCCESS in Xs]
3 of 3 OK   created materialized_table model high_value_orders [SUCCESS in Xs]
```

**Say:** "In the Confluent Cloud console under Flink → Statements, you can see those statements running and the output topics updating in real time."

*(Switch to browser, show the Confluent Cloud console.)*

---

## [1:15–1:35] Tests and docs still work

### Tests

```bash
dbt test
```

**Say:** "Because this is still dbt, all the guardrails come with it. I can run `dbt test` to execute data tests and unit tests against these streaming models and get deterministic results — not flaky timeouts."

### Docs

```bash
dbt docs generate && dbt docs serve
```

Open `http://localhost:8080`.

**Say:** "And when I run `dbt docs generate`, the adapter plugs into Flink's INFORMATION_SCHEMA, so the dbt docs site stays up to date for these streaming pipelines — full lineage from the Faker source through staging to the mart."

---

## [Optional beat] Change the logic, re-run

*(Use this beat any time you want to show MT vs CTAS. It slots in here or right after the streaming query below — presenter's choice.)*

### Edit the threshold

Open `models/marts/high_value_orders.sql` and change the WHERE clause:

```diff
- WHERE amount >= 300
+ WHERE amount >= 200
```

**Say:** "The value of a materialized table is that its query is **persisted as part of the table's metadata** and Flink owns the refresh pipeline. With a hand-rolled CTAS, the SELECT and the job that populates it are decoupled — you'd be stopping a streaming job, dropping the topic, and re-submitting an INSERT yourself. Here I just change the SQL like any other dbt model."

### Re-run

```bash
dbt run --full-refresh -s high_value_orders
```

**Say:** "dbt detects the existing materialized table, deletes its Flink statement, drops the table, and re-creates it with the new query — same `FRESHNESS = INTERVAL '1' MINUTE` contract, new logic."

*(Switch to Confluent Cloud → Flink → Statements — show the old statement gone, new one running.)*

### Confirm the change

```bash
python python/streaming_query.py
```

**Say:** "More orders pass the lower threshold, so the live feed visibly speeds up. Same workflow, new logic — that's the dbt + materialized table loop."

---

## [1:35–1:55] confluent_sql: everything else in Python

### Snapshot query

```bash
python python/snapshot_query.py
```

**Say:** "Under the hood, the adapter uses a new `confluent_sql` Python driver, and you can use it directly. Here I connect with `confluent_sql.connect(...)`, run a snapshot query with `pd.read_sql`, and load the result straight into pandas."

Expected output:
```
Connected to Confluent Cloud Flink.
Running snapshot query on stg_orders...

 order_id  customer_id  product_id  product_name          status     amount
   482103          317          12  Rustic Cotton Shirt    placed    123.45
   719204           88          31  Sleek Wooden Table     shipped   287.00
...
20 rows returned.
```

**Say:** "Because it's DB-API v2 compliant, it plugs into tools like Airflow, Dagster, Streamlit, LangChain, Hex, and Metabase — anything that speaks standard Python database connections."

### Streaming query

```bash
python python/streaming_query.py
```

*(Let it run for 20–30 seconds before Ctrl+C.)*

**Say:** "I can also open a streaming query to consume results continuously. Each line is a high-value order — the materialized table emits a new row as soon as it passes the filter."

Expected output:
```
  order_id product_name                            amount              event_time
------------------------------------------------------------------------------------
[09:14:22]     482103 Rustic Cotton Shirt                $   412.50 2026-05-06 09:14:20
[09:14:23]     719204 Sleek Wooden Table                 $   387.00 2026-05-06 09:14:22
[09:14:24]     482877 Premium Walnut Desk                $   478.25 2026-05-06 09:14:23
...
```

Press `Ctrl+C` to stop cleanly.

---

## [1:55–2:00] Close

**Say:** "So, if your team already lives in dbt, you can now reuse that same workflow to run real-time Flink pipelines on Confluent Cloud. Change a `WHERE` clause, run dbt, and the materialized table re-deploys with the same freshness SLO — no streaming-job lifecycle management. And the rest of your Python ecosystem plugs in through `confluent_sql`, without learning a new platform or rewriting your pipelines."

---

## Troubleshooting

| Symptom | Check |
|---|---|
| `confluent` missing from `dbt --version` | Run `pip install dbt-confluent` |
| `dbt run` auth error | Use Flink Region API keys (not control-plane keys); create under Flink → Compute Pools → API Keys |
| `dbt run` schema error | `CONFLUENT_DBNAME` must match an existing Kafka cluster name in your environment |
| Compute pool error | Pool must be in `RUNNING` state; `PROVISIONING` pools reject statements |
| Python `ModuleNotFoundError` | Run `pip install -r requirements.txt` |
| Python `Missing environment variables` | Copy `.env.example` to `.env` and fill in all fields |
| Empty streaming output | The materialized_table refreshes at its FRESHNESS interval; first results appear within ~1 minute of `dbt run` |
