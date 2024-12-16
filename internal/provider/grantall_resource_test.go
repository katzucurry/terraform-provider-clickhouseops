// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

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
					resource.TestCheckResourceAttr("clickhouseops_grantall.new_grant_all", "assignee", "user3"),
				),
			},
		},
	})
}

const testAccGrantAllConfig = `
resource "clickhouseops_simpleuser" "user3" {
	name = "user3"
	sha256_password = sha256("password3")
}

resource "clickhouseops_grantall" "new_grant_all" {
	assignee = clickhouseops_simpleuser.user3.name
}
`
