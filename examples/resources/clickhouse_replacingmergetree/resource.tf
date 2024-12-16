provider "clickhouseops" {}

resource "clickhouseops_database" "new_database" {
  name    = "new_db"
  comment = "new db test comment"
}

resource "clickhouseops_replacingmergetree" "new_table" {
  name          = "new_table"
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
