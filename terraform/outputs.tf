output "organization_id" {
  value = data.confluent_organization.this.id
}

output "environment_id" {
  value = confluent_environment.this.id
}

output "kafka_cluster_name" {
  value = confluent_kafka_cluster.this.display_name
}

output "compute_pool_id" {
  value = confluent_flink_compute_pool.this.id
}

output "cloud_provider" {
  value = lower(var.cloud)
}

output "cloud_region" {
  value = var.region
}

output "flink_api_key" {
  value     = confluent_api_key.flink.id
  sensitive = true
}

output "flink_api_secret" {
  value     = confluent_api_key.flink.secret
  sensitive = true
}

output "env_file" {
  description = "Paste-ready .env block. Retrieve with: terraform output -raw env_file"
  sensitive   = true
  value       = <<-ENV
    export CONFLUENT_ORG_ID=${data.confluent_organization.this.id}
    export CONFLUENT_ENV_ID=${confluent_environment.this.id}
    export CONFLUENT_CLOUD_PROVIDER=${lower(var.cloud)}
    export CONFLUENT_CLOUD_REGION=${var.region}
    export CONFLUENT_FLINK_API_KEY=${confluent_api_key.flink.id}
    export CONFLUENT_FLINK_API_SECRET=${confluent_api_key.flink.secret}
    export CONFLUENT_COMPUTE_POOL_ID=${confluent_flink_compute_pool.this.id}
    export CONFLUENT_DBNAME=${confluent_kafka_cluster.this.display_name}
    export DBT_PROFILES_DIR=$PWD/.dbt
  ENV
}
