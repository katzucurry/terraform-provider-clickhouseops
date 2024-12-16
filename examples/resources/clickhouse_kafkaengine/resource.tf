provider "clickhouseops" {}

resource "clickhouseops_database" "new_database" {
  name    = "new_db"
  comment = "new db test comment"
}

resource "clickhouseops_kafkaengine" "new_table2" {
  name          = "test_kafka_engine"
  database_name = clickhouse_database.new_database.name
  columns = [{
    name = "a"
    type = "String"
    }, {
    name = "b"
    type = "String"
  }]
  kafka_broker_list               = "kafka:9092"
  kafka_topic_list                = "topic1"
  kafka_group_name                = "topic1.group"
  kafka_format                    = "AvroConfluent"
  format_avro_schema_registry_url = "http://schema-registry:8081"
}