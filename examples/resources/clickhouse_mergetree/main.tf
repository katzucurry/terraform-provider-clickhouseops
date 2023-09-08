terraform {
  required_providers {
    clickhouse = {
      version = "0.1.0"
      source  = "hashicorp.com/awesomenessnil/clickhouse"
    }
  }
}

provider "clickhouse" {}

resource "clickhouse_database" "new_database" {
  name    = "new_db"
  comment = "new db test comment"
}

resource "clickhouse_mergetree" "new_table1" {
  name          = "test_merge_tree"
  database_name = clickhouse_database.new_database.name
  columns = [{
    name = "a"
    type = "String"
    }, {
    name = "b"
    type = "String"
  }]
  order_by = "a"
}