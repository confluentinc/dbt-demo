variable "cloud" {
  type        = string
  default     = "AWS"
  description = "Confluent cloud provider (uppercase for resources; outputs lowercase it for the demo's .env)."
}

variable "region" {
  type        = string
  default     = "us-east-2"
  description = "Cloud region for the Kafka cluster and Flink compute pool."
}

variable "prefix" {
  type        = string
  default     = "dbt-demo"
  description = "Naming prefix applied to the environment, cluster, compute pool, and service account."
}

variable "compute_pool_max_cfu" {
  type        = number
  default     = 5
  description = "Maximum Confluent Flink Units for the compute pool."

  validation {
    condition     = contains([5, 10, 20, 30, 40, 50], var.compute_pool_max_cfu)
    error_message = "compute_pool_max_cfu must be one of 5, 10, 20, 30, 40, 50."
  }
}
