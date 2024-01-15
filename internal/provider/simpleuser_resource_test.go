package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccSimpleUserResource(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccSimpleUserConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("clickhouse_simpleuser.new_user", "name", "new_user"),
				),
			},
		},
	})
}

const testAccSimpleUserConfig = `
resource "clickhouse_simpleuser" "new_user" {
  name = "new_user"
  sha256_password = "dummy_password"
}
`
