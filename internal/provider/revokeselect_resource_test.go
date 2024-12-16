// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccRevokeSelectResource(t *testing.T) {
	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccRevokeSelectConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("clickhouseops_revokeselect.new_revoke", "assignee", "user2"),
				),
			},
		},
	})
}

const testAccRevokeSelectConfig = `
resource "clickhouseops_simpleuser" "user2" {
	name = "user2"
	sha256_password = sha256("password2")
}

resource "clickhouseops_revokeselect" "new_revoke" {
	database_name = "system"
	table_name = "tables"
	columns_name = ["database", "name"]
	assignee = clickhouseops_simpleuser.user2.name
}
`
