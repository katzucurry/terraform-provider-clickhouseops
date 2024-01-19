package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccGrantRoleResource(t *testing.T) {
	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccGrantRoleConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("clickhouse_grantrole.user1_role1", "user_name", "user1"),
					resource.TestCheckResourceAttr("clickhouse_grantrole.user1_role1", "role_name", "role1"),
				),
			},
		},
	})
}

const testAccGrantRoleConfig = `
resource "clickhouse_simpleuser" "user1" {
	name = "user1"
	sha256_password = "password1"
}

resource "clickhouse_simplerole" "role1" {
	name = "role1"
}

resource "clickhouse_grantrole" "user1_role1" {
	user_name = clickhouse_simpleuser.user1.name
	role_name = clickhouse_simplerole.role1.name
}
`
