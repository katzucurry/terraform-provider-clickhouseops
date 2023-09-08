terraform {
  required_providers {
    clickhouse = {
      version = "0.1.0"
      source  = "hashicorp.com/awesomenessnil/clickhouse"
    }
  }
}

provider "clickhouse" {}

resource "clickhouse_database" "test" {
  name    = "test"
  comment = "test comment"
}