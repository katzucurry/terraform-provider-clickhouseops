provider "clickhouse" {}

resource "clickhouse_database" "test" {
  name = "test"
}

resource "clickhouse_view" "new_view" {
  name          = "new_view"
  database_name = clickhouse_database.test.name
  sql           = <<EOT
SELECT
    event_time,
    type,
    query,
    initial_user
FROM system.query_log
ORDER BY event_time DESC
LIMIT 10
EOT
}
