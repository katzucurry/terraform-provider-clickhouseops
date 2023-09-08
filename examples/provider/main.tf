terraform {
  required_providers {
    clickhouse = {
      version = "0.1.0"
      source  = "hashicorp.com/awesomenessnil/clickhouse"
    }
  }
}
provider "clickhouse" {}

data "clickhouse_database" "example" {}