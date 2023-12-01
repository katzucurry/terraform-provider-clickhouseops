resource "clickhouse_database" "source" {
  name = "source"
}

resource "clickhouse_database" "target" {
  name = "target"
}

resource "clickhouse_materializedview" "new_view" {
  name                 = "test"
  database_name        = clickhouse_database.source.name
  target_database_name = clickhouse_database.target.name
  target_table_name    = "test"
  sql                  = <<EOT
SELECT 1
EOT
}