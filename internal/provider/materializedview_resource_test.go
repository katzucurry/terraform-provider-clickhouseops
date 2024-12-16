// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

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
					resource.TestCheckResourceAttr("clickhouseops_materializedview.new_view", "name", "test"),
				),
			},
		},
	})
}

const testAccMaterializedViewResourceConfig = `
resource "clickhouseops_database" "source" {
	name = "source"
}

resource "clickhouseops_database" "target" {
	name = "target"
}

resource "clickhouseops_materializedview" "new_view" {
	name = "test"
	database_name = clickhouseops_database.source.name
	target_database_name = clickhouseops_database.target.name
	target_table_name = "test"
	sql = <<EOT
SELECT 1
EOT
}
`
