resource "clickhouse_database" "new_database" {
  name = "new_database"
}

resource "clickhouse_postgresql" "new_table" {
  name          = "new_table"
  database_name = clickhouse_database.new_database.name
  columns = [{
    name = "a"
    type = "String"
  }]
  postgresql_host          = "localhost"
  postgresql_port          = 5432
  postgresql_database_name = "postgres"
  postgresql_table_name    = "test"
  postgresql_username      = "user"
  postgresql_password      = "password"
}