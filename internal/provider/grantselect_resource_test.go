package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccGrantSelectResource(t *testing.T) {
	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccGrantSelectConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("clickhouse_grantselect.new_grant", "assignee", "user1"),
				),
			},
		},
	})
}

const testAccGrantSelectConfig = `
resource "clickhouse_simpleuser" "user1" {
	name = "user1"
	sha256_password = "password1"
}

resource "clickhouse_grantselect" "new_grant" {
	database_name = "system"
	table_name = "tables"
	columns_name = ["database", "name"]
	assignee = clickhouse_simpleuser.user1.name
}
`
