package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccNamedCollectionResource(t *testing.T) {
	t.Skip("This test is not supported in CI environment")
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccNamedCollectionResourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("clickhouse_namedcollection.test", "name", "test"),
				),
			},
		},
	})
}

const testAccNamedCollectionResourceConfig = `
resource "clickhouse_database" "test" {
	name = "test"
}

resource "clickhouse_namedcollection" "test" {
  name = "test"
  keyvaluepairs = [{
	key = "test_key1"
	value = "test_value1"
	is_not_overridable = true
  },{
	key = "test_key2"
	value = "test_value2"
  }]
}
`
