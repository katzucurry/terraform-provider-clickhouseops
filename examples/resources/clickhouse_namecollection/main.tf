resource "clickhouse_database" "test" {
  name = "test"
}

resource "clickhouse_namedcollection" "test" {
  name = "test"
  keyvaluepairs = [{
    key                = "test_key1"
    value              = "test_value1"
    is_not_overridable = true
    }, {
    key   = "test_key2"
    value = "test_value2"
  }]
}