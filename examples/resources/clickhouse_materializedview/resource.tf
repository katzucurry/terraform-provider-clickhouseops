resource "clickhouseops_database" "source" {
  name = "source"
}

resource "clickhouseops_database" "target" {
  name = "target"
}

resource "clickhouseops_materializedview" "new_view" {
  name                 = "test"
  database_name        = clickhouse_database.source.name
  target_database_name = clickhouse_database.target.name
  target_table_name    = "test"
  sql                  = <<EOT
SELECT 1
EOT
}