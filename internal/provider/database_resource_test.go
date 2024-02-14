package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccDatabaseResource(t *testing.T) {
	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: providerConfig + `
resource "clickhouseops_database" "test" {
	name = "test"
	comment = "test comment"
}				
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("clickhouseops_database.test", "name", "test"),
					resource.TestCheckResourceAttr("clickhouseops_database.test", "comment", "test comment"),
				),
			},
		},
	})
}
