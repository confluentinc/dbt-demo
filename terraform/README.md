# Terraform: Confluent Cloud infrastructure for dbt-demo

Provisions everything the dbt-demo needs to run end-to-end on Confluent Cloud
Flink:

- A Confluent Cloud environment (Stream Governance package: `ESSENTIALS`)
- A Standard Kafka cluster in AWS `us-east-2`
- A Flink compute pool (5 CFU)
- A service account with `FlinkDeveloper` on the environment
- A Flink **region** API key bound to that service account

One `terraform apply` brings the whole stack up. One `terraform destroy` tears
it back down.

## Prerequisites

- `terraform >= 1.5`
- A Confluent Cloud **Cloud API key** with `OrganizationAdmin` (this is the
  control-plane key the Terraform provider uses — it is *not* the same thing
  as the Flink region key the demo consumes at runtime)

## Apply

```bash
cd terraform

export CONFLUENT_CLOUD_API_KEY=...
export CONFLUENT_CLOUD_API_SECRET=...

terraform init
terraform plan      # expect ~7 resources to add
terraform apply     # 5-10 min, mostly cluster provisioning
```

## Populate the demo's `.env`

The `env_file` output produces a ready-to-paste block matching `.env.example`:

```bash
terraform output -raw env_file > ../.env
cd ..
source .env
dbt debug           # confirms Flink connectivity
dbt run
```

If `dbt debug` fails immediately after apply, wait 30-60 seconds — newly
issued Flink region keys can take a moment to propagate, then retry.

## Teardown

```bash
cd terraform
terraform destroy
```

## Customization

All inputs have sensible defaults; override on the command line if needed:

| Variable                | Default      | Notes                                                     |
| ----------------------- | ------------ | --------------------------------------------------------- |
| `cloud`                 | `AWS`        | Uppercase. Outputs lowercase it for the demo's `.env`.    |
| `region`                | `us-east-2`  | Must be a Flink-enabled region for the chosen cloud.      |
| `prefix`                | `dbt-demo`   | Naming prefix for env, cluster, pool, service account.    |
| `compute_pool_max_cfu`  | `5`          | Valid: 5, 10, 20, 30, 40, 50.                             |

To upgrade Stream Governance to `ADVANCED` (Data Catalog, Lineage — billable),
edit the `stream_governance` block in `main.tf`.

## Security note

`terraform.tfstate` contains the Flink API secret in plaintext. The
`.gitignore` here excludes state files, but treat the directory as sensitive
and do not share it. A remote state backend (S3 + KMS, Terraform Cloud, etc.)
is recommended for anything beyond a personal demo.
