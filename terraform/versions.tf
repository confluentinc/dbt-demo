terraform {
  required_version = ">= 1.5.0"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.73"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.11"
    }
  }
}
