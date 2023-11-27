provider "clickhouse" {}

resource "clickhouse_database" "test" {
  name    = "test"
  comment = "test comment"
}