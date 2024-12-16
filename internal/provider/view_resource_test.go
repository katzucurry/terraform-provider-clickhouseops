// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

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
resource "clickhouseops_database" "new_database" {
	name = "new_db"
	comment = "new db test comment"
}		

resource "clickhouseops_view" "new_view" {
	name = "test"
	database_name = clickhouseops_database.new_database.name
	sql = <<EOT
SELECT 1
EOT
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("clickhouseops_view.new_view", "name", "test"),
					resource.TestCheckResourceAttr("clickhouseops_view.new_view", "database_name", "new_db"),
					resource.TestCheckResourceAttr("clickhouseops_view.new_view", "sql", "SELECT 1\n"),
				),
			},
		},
	})
}
