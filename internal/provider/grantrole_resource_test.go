// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

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
					resource.TestCheckResourceAttr("clickhouseops_grantrole.user1_role1", "user_name", "user1"),
					resource.TestCheckResourceAttr("clickhouseops_grantrole.user1_role1", "role_name", "role1"),
				),
			},
		},
	})
}

const testAccGrantRoleConfig = `
resource "clickhouseops_simpleuser" "user1" {
	name = "user1"
	sha256_password = sha256("password1")
}

resource "clickhouseops_simplerole" "role1" {
	name = "role1"
}

resource "clickhouseops_grantrole" "user1_role1" {
	user_name = clickhouseops_simpleuser.user1.name
	role_name = clickhouseops_simplerole.role1.name
}
`
