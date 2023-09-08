package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccViewResource(t *testing.T) {
	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: providerConfig + `
resource "clickhouse_database" "new_database" {
	name = "new_db"
	comment = "new db test comment"
}		

resource "clickhouse_view" "new_view" {
	name = "test"
	database_name = clickhouse_database.new_database.name
	sql = <<EOT
SELECT 1
EOT
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("clickhouse_view.new_view", "name", "test"),
					resource.TestCheckResourceAttr("clickhouse_view.new_view", "database_name", "new_db"),
					resource.TestCheckResourceAttr("clickhouse_view.new_view", "sql", "SELECT 1\n"),
				),
			},
		},
	})
}
