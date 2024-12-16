// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

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
					resource.TestCheckResourceAttr("clickhouseops_namedcollection.test", "name", "test"),
				),
			},
		},
	})
}

const testAccNamedCollectionResourceConfig = `
resource "clickhouseops_namedcollection" "test" {
	name = "test"
	keyvaluepairs = [{
	  key = "host"
	  value = "localhost"
	},{
	  key = "port"
	  value = "5432"
	},{
	  key = "user"
	  value = "user"
	}]
	sensitive_keyvaluepairs = [{
	  key = "password"
	  value = "password"
	}]
  }
`
