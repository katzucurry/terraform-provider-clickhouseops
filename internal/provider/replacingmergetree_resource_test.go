package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccReplacingMergeTreeResource(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccReplacingMergeTreeResourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("clickhouse_replacingmergetree.test", "name", "test"),
				),
			},
		},
	})
}

const testAccReplacingMergeTreeResourceConfig = `
resource "clickhouse_database" "test" {
	name = "test"
}

resource "clickhouse_replacingmergetree" "test" {
  name = "test"
  database_name = clickhouse_database.test.name
  columns = [{
	name = "a"
	type = "String"
  },{
	name = "b"
	type = "String"
  }]
  order_by = "a"
}
`
