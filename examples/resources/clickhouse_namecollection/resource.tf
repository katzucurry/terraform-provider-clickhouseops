resource "clickhouse_database" "test" {
  name = "test"
}

resource "clickhouse_namedcollection" "test" {
  name = "test"
  keyvaluepairs = [{
    key   = "host"
    value = "localhost"
    }, {
    key   = "port"
    value = "5432"
    }, {
    key   = "user"
    value = "user"
  }]
  sensitive_keyvaluepairs = [{
    key   = "password"
    value = "password"
  }]
}