package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccMaterializedViewResource(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccMaterializedViewResourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("clickhouse_materializedview.new_view", "name", "test"),
				),
			},
		},
	})
}

const testAccMaterializedViewResourceConfig = `
resource "clickhouse_database" "source" {
	name = "source"
}

resource "clickhouse_database" "target" {
	name = "target"
}

resource "clickhouse_materializedview" "new_view" {
	name = "test"
	database_name = clickhouse_database.source.name
	target_database_name = clickhouse_database.target.name
	target_table_name = "test"
	sql = <<EOT
SELECT 1
EOT
}
`
