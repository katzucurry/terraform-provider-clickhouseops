// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/awesomenessnil/terraform-provider-clickhouse/internal/common"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

// Ensure provider defined types fully satisfy framework interfaces.
var _ datasource.DataSource = &S3DescribeDataSource{}

func NewS3DescribeDataSource() datasource.DataSource {
	return &S3DescribeDataSource{}
}

// S3DescribeDataSource defines the data source implementation.
type S3DescribeDataSource struct {
	db *sql.DB
}

// S3DescribeDataSourceModel describes the data source data model.

type S3DescribeDataSourceModel struct {
	Id                  types.String       `tfsdk:"id"`
	NamedCollectionName types.String       `tfsdk:"named_collection_name"`
	Path                types.String       `tfsdk:"path"`
	NoSign              types.Bool         `tfsdk:"nosign"`
	AwsAccessKeyId      types.String       `tfsdk:"aws_access_key_id"`
	AwsSecretAccessKey  types.String       `tfsdk:"aws_secret_access_key"`
	Format              types.String       `tfsdk:"format"`
	Compression         types.String       `tfsdk:"compression"`
	ClickhouseColumns   []ClickhouseColumn `tfsdk:"clickhouse_columns"`
}

type ClickhouseColumn struct {
	Name types.String `tfsdk:"name"`
	Type types.String `tfsdk:"type"`
}

type Column struct {
	Name              string
	Type              string
	DefaultType       string
	DefaultExpression string
	Comment           string
	CodecExpression   string
	TTLExpression     string
}

func (d *S3DescribeDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_s3describe"
}

func (d *S3DescribeDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		// This description is used by the documentation generator and the language server.
		MarkdownDescription: "Data source to retrieve schema using Clickhouse s3 function and describe clause",

		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				MarkdownDescription: "ID identify the resource",
				Computed:            true,
			},
			"named_collection_name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Named Collection containing the configuration for S3",
				Optional:            true,
			},
			"path": schema.StringAttribute{
				MarkdownDescription: "S3 path containing the data",
				Required:            true,
			},
			"nosign": schema.BoolAttribute{
				MarkdownDescription: "If it is true all the requests will not be signed",
				Optional:            true,
			},
			"aws_access_key_id": schema.StringAttribute{
				MarkdownDescription: "aws_access_key_id with permission for s3",
				Optional:            true,
			},
			"aws_secret_access_key": schema.StringAttribute{
				MarkdownDescription: "aws_secret_access_key with permission for s3",
				Optional:            true,
			},
			"format": schema.StringAttribute{
				MarkdownDescription: "data format, ie. CSV, Parquet, etc.",
				Optional:            true,
			},
			"compression": schema.StringAttribute{
				MarkdownDescription: "data compression format, ie. gzip, zip, etc.",
				Optional:            true,
			},
			"clickhouse_columns": schema.ListNestedAttribute{
				MarkdownDescription: "PostgreSQL columns converted to Clickhouse columns",
				Computed:            true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							MarkdownDescription: "Column name",
							Computed:            true,
						},
						"type": schema.StringAttribute{
							MarkdownDescription: "Clickhouse type",
							Computed:            true,
						},
					},
				},
			},
		},
	}
}

func (d *S3DescribeDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	db, ok := req.ProviderData.(*sql.DB)

	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *sql.DB, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)

		return
	}

	d.db = db
}

/*
DESCRIBE s3(path [, NOSIGN | aws_access_key_id, aws_secret_access_key [,session_token]] [,format] [,structure] [,compression])
*/

const s3DescribeTemplate = `
DESCRIBE s3(
	{{if not .NamedCollectionName.IsNull}}{{.NamedCollectionName.ValueString}}
	{{if not .Path.IsNull}},path='{{.Path.ValueString}}'{{end}}
	{{if not .NoSign.IsNull}}{{if .NoSign.ValueBool}},NOSIGN='NOSIGN'{{end}}{{end}}
	{{if not .AwsAccessKeyId.IsNull}},aws_access_key_id='{{.AwsAccessKeyId.ValueString}}'{{end}}
	{{if not .AwsSecretAccessKey.IsNull}},aws_secret_access_key='{{.AwsSecretAccessKey.ValueString}}'{{end}}
	{{if not .Format.IsNull}},format='{{.Format.ValueString}}'{{end}}
	{{if not .Compression.IsNull}},compression='{{.Compression.ValueString}}'{{end}}
	{{else}}
	'{{.Path.ValueString}}'
	{{if not .NoSign.IsNull}}{{if .NoSign.ValueBool}},NOSIGN{{end}}{{end}}
	{{if not .AwsAccessKeyId.IsNull}},'{{.AwsAccessKeyId.ValueString}}'{{end}}
	{{if not .AwsSecretAccessKey.IsNull}},'{{.AwsSecretAccessKey.ValueString}}'{{end}}
	{{if not .Format.IsNull}},'{{.Format.ValueString}}'{{end}}
	{{if not .Compression.IsNull}},'{{.Compression.ValueString}}'{{end}}
	{{end}})
`

func (d *S3DescribeDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var data S3DescribeDataSourceModel

	// Read Terraform configuration data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	if data.NamedCollectionName.IsNull() && data.Path.IsNull() {
		resp.Diagnostics.AddError(
			"Missing Attribute Configuration",
			"Expect a Clickhouse named collection or at least a Path defined",
		)
		return
	}

	query, err := common.RenderTemplate(s3DescribeTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error trying to get Clickhouse columns from S3 path",
			"Could not render DESCRIBE, unexpected error: "+err.Error(),
		)
		return
	}

	rows, err := d.db.Query(*query)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error trying to get Clickhouse columns from S3 path",
			"Could not execute DESCRIBE: unexpected error: "+err.Error(),
		)
		return
	}
	defer rows.Close()

	var columns []ClickhouseColumn
	for rows.Next() {
		var col Column
		if err := rows.Scan(&col.Name, &col.Type, &col.DefaultType, &col.DefaultExpression,
			&col.Comment, &col.CodecExpression, &col.TTLExpression); err != nil {
			resp.Diagnostics.AddError(
				"Error trying to get Clickhouse columns from S3 path",
				"Could not retrieve Rows: unexpected error: "+err.Error(),
			)
			return
		}

		columns = append(columns, ClickhouseColumn{
			Name: types.StringValue(col.Name),
			Type: types.StringValue(col.Type),
		})
	}

	data.Id = data.Path
	data.ClickhouseColumns = columns

	// Write logs using the tflog package
	// Documentation: https://terraform.io/plugin/log
	tflog.Trace(ctx, "read a data source")

	// Save data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}
