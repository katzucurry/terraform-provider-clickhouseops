package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccDistributedResource(t *testing.T) {
	t.Skip("This test is not supported in CI environment")
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testDistributedConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("clickhouseops_distributed.new_table_dist", "name", "new_table_dist"),
				),
			},
		},
	})
}

const testDistributedConfig = `
resource "clickhouseops_distributed" "new_table_dist" {
  name = "new_table_dist"
  database_name = "default"
  columns = [{
	name = "name"
	type = "String"
  },{
	name = "value"
	type = "String"
  }]
  dist_cluster = "clickhouse"
  dist_database = "system"
  dist_table = "build_options"
}
`
