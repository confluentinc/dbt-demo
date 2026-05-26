provider "confluent" {}

data "confluent_organization" "this" {}

data "confluent_flink_region" "this" {
  cloud  = var.cloud
  region = var.region
}

resource "confluent_environment" "this" {
  display_name = "${var.prefix}-env"

  stream_governance {
    package = "ESSENTIALS"
  }
}

resource "confluent_kafka_cluster" "this" {
  display_name = "${var.prefix}-cluster"
  availability = "SINGLE_ZONE"
  cloud        = var.cloud
  region       = var.region

  standard {}

  environment {
    id = confluent_environment.this.id
  }
}

resource "confluent_flink_compute_pool" "this" {
  display_name = "${var.prefix}-pool"
  cloud        = var.cloud
  region       = var.region
  max_cfu      = var.compute_pool_max_cfu

  environment {
    id = confluent_environment.this.id
  }
}

resource "confluent_service_account" "flink" {
  display_name = "${var.prefix}-flink-sa"
  description  = "Runs Flink statements for the dbt-demo"
}

resource "confluent_role_binding" "flink_developer" {
  principal   = "User:${confluent_service_account.flink.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = confluent_environment.this.resource_name
}

# Grants the Flink SA visibility of the Kafka cluster as a Flink schema and
# the topic create/read/write rights dbt needs to materialize tables.
resource "confluent_role_binding" "flink_cluster_admin" {
  principal   = "User:${confluent_service_account.flink.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.this.rbac_crn
}

# Stream Governance provisions the SR cluster asynchronously; the env can
# report ready before the SR lookup succeeds. Pause to let it settle.
resource "time_sleep" "wait_for_sr" {
  depends_on      = [confluent_environment.this]
  create_duration = "60s"
}

data "confluent_schema_registry_cluster" "essentials" {
  environment {
    id = confluent_environment.this.id
  }

  depends_on = [time_sleep.wait_for_sr]
}

# Materialized tables use the avro-registry format and dbt drops temp
# schema-check tables, so the Flink SA needs full lifecycle on SR subjects.
resource "confluent_role_binding" "flink_sr_owner" {
  principal   = "User:${confluent_service_account.flink.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.essentials.resource_name}/subject=*"
}

resource "confluent_api_key" "flink" {
  display_name = "${var.prefix}-flink-key"
  description  = "Flink region API key for the dbt-demo"

  owner {
    id          = confluent_service_account.flink.id
    api_version = confluent_service_account.flink.api_version
    kind        = confluent_service_account.flink.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.this.id
    api_version = data.confluent_flink_region.this.api_version
    kind        = data.confluent_flink_region.this.kind

    environment {
      id = confluent_environment.this.id
    }
  }

  depends_on = [confluent_role_binding.flink_developer]
}
