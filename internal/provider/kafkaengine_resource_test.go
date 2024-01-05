package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccKafkaEngineResource(t *testing.T) {
	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: providerConfig + testAccKafkaResourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("clickhouse_kafkaengine.new_kafka_engine_table", "name", "test_kafka_engine"),
				),
			},
			{
				Config: providerConfig + testAccKafkaResourceWithNamedCollectionConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("clickhouse_kafkaengine.new_kafka_engine_table", "name", "test_kafka_engine"),
				),
			},
		},
	})
}

const (
	testAccKafkaResourceConfig = `
resource "clickhouse_database" "new_database" {
	name = "new_db"
	comment = "new db test comment"
}		

resource "clickhouse_kafkaengine" "new_kafka_engine_table" {
	name = "test_kafka_engine"
	database_name = clickhouse_database.new_database.name
	columns = [{
		name = "a"
		type = "String"
	},{
		name = "b"
		type = "String"
	}]
	kafka_broker_list = "confluent-cp-kafka-headless:9092"
	kafka_topic_list = "postgres.public.test"
	kafka_group_name = "postgres.public.test.group"
	kafka_format = "AvroConfluent"
	format_avro_schema_registry_url = "http://confluent-cp-schema-registry:8081"
}
`
	testAccKafkaResourceWithNamedCollectionConfig = `
resource "clickhouse_database" "new_database" {
	name = "new_db"
	comment = "new db test comment"
}		

resource "clickhouse_namedcollection" "new_config" {
	name = "new_config"
	keyvaluepairs = [{
	  key = "kafka_broker_list"
	  value = "confluent-cp-kafka-headless:9092"
	},{
	  key = "kafka_format"
	  value = "AvroConfluent"
	},{
	  key = "format_avro_schema_registry_url"
	  value = "http://confluent-cp-schema-registry:8081"
	}]
}

resource "clickhouse_kafkaengine" "new_kafka_engine_table" {
	name = "test_kafka_engine"
	database_name = clickhouse_database.new_database.name
	columns = [{
		name = "a"
		type = "String"
	},{
		name = "b"
		type = "String"
	}]
	named_collection_name = clickhouse_namedcollection.new_config.name
	kafka_topic_list = "postgres.public.test"
	kafka_group_name = "postgres.public.test.group"
}
`
)
