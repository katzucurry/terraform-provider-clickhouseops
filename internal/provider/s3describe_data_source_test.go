// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccS3DescribeDataSource(t *testing.T) {
	t.Skip("This test is not supported in CI environment")
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccS3DescribeConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.clickhouseops_s3describe.addresses", "clickhouseops_columns.0.name", "c1"),
					resource.TestCheckResourceAttr("data.clickhouseops_s3describe.addresses", "clickhouseops_columns.0.type", "Nullable(String)"),
					resource.TestCheckResourceAttr("data.clickhouseops_s3describe.addresses", "clickhouseops_columns.1.name", "c2"),
					resource.TestCheckResourceAttr("data.clickhouseops_s3describe.addresses", "clickhouseops_columns.1.type", "Nullable(String)"),
					resource.TestCheckResourceAttr("data.clickhouseops_s3describe.addresses", "clickhouseops_columns.2.name", "c3"),
					resource.TestCheckResourceAttr("data.clickhouseops_s3describe.addresses", "clickhouseops_columns.2.type", "Nullable(String)"),
					resource.TestCheckResourceAttr("data.clickhouseops_s3describe.addresses", "clickhouseops_columns.3.name", "c4"),
					resource.TestCheckResourceAttr("data.clickhouseops_s3describe.addresses", "clickhouseops_columns.3.type", "Nullable(String)"),
					resource.TestCheckResourceAttr("data.clickhouseops_s3describe.addresses", "clickhouseops_columns.4.name", "c5"),
					resource.TestCheckResourceAttr("data.clickhouseops_s3describe.addresses", "clickhouseops_columns.4.type", "Nullable(String)"),
					resource.TestCheckResourceAttr("data.clickhouseops_s3describe.addresses", "clickhouseops_columns.5.name", "c6"),
					resource.TestCheckResourceAttr("data.clickhouseops_s3describe.addresses", "clickhouseops_columns.5.type", "Nullable(Int64)"),
				),
			},
		},
	})
}

const testAccS3DescribeConfig = `
data "clickhouseops_s3describe" "addresses" {
	path = "http://minio:9000/test/addresses.csv"
	aws_access_key_id = "minioadmin"
	aws_secret_access_key = "minioadmin"
	format = "CSV"
}
`
