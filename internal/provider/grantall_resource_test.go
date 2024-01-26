package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccGrantAllResource(t *testing.T) {
	t.Skip("This test is not supported in CI environment")
	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccGrantAllConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("clickhouse_grantall.new_grant_all", "assignee", "user3"),
				),
			},
		},
	})
}

const testAccGrantAllConfig = `
resource "clickhouse_simpleuser" "user3" {
	name = "user3"
	sha256_password = sha256("password3")
}

resource "clickhouse_grantall" "new_grant_all" {
	assignee = clickhouse_simpleuser.user3.name
}
`
