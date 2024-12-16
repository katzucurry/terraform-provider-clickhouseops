// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/katzucurry/terraform-provider-clickhouseops/internal/common"
)

var (
	_ resource.Resource                = &DatabaseResource{}
	_ resource.ResourceWithConfigure   = &DatabaseResource{}
	_ resource.ResourceWithImportState = &DatabaseResource{}
)

func NewDatabaseResource() resource.Resource {
	return &DatabaseResource{}
}

type DatabaseResource struct {
	db clickhouse.Conn
}

type DatabaseResourceModel struct {
	ID          types.String `tfsdk:"id"`
	Name        types.String `tfsdk:"name"`
	Comment     types.String `tfsdk:"comment"`
	ClusterName types.String `tfsdk:"cluster_name"`
}

func (r *DatabaseResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database"
}

func (r *DatabaseResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Clickhouse database",

		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse database name",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"comment": schema.StringAttribute{
				MarkdownDescription: "Clickhouse database comment",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"cluster_name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse cluster to execute DDL",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
		},
	}
}

func (r *DatabaseResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	db, ok := req.ProviderData.(clickhouse.Conn)

	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected clickhouse.Conn, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	r.db = db
}

func (r *DatabaseResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data *DatabaseResourceModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	queryTemplate := `CREATE DATABASE IF NOT EXISTS "{{.Name.ValueString}}" {{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}} {{if not .Comment.IsNull}} COMMENT '{{.Comment.ValueString}}'{{end}}`
	query, err := common.RenderTemplate(queryTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse Database",
			"Could not render DDL, unexpected error: "+err.Error(),
		)
		return
	}

	err = r.db.Exec(ctx, *query)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse Database",
			"Could not execute DDL, unexpected error: "+err.Error(),
		)
		return
	}

	data.ID = types.StringValue(data.ClusterName.ValueString() + ":" + data.Name.ValueString())

	tflog.Trace(ctx, "Created a Database resource")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *DatabaseResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data *DatabaseResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if !r.existsDatabase(ctx, data.Name.ValueString(), data.ClusterName.ValueString()) {
		tflog.Trace(ctx, "Database doesn't exists anymore, it should recreate it")
		resp.State.RemoveResource(ctx)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *DatabaseResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data *DatabaseResourceModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *DatabaseResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data *DatabaseResourceModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	queryTemplate := `DROP DATABASE "{{.Name.ValueString}}" {{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}}`
	query, err := common.RenderTemplate(queryTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Deleting Clickhouse Database",
			"Could not render DDL, unexpected error: "+err.Error(),
		)
		return
	}

	err = r.db.Exec(ctx, *query)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Deleting Clickhouse Database",
			"Could not execute DDL, unexpected error: "+err.Error(),
		)
		return
	}
}

func (r *DatabaseResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

func (r *DatabaseResource) existsDatabase(ctx context.Context, databaseName string, clusterName string) bool {
	if clusterName != "" {
		return r.existsDatabaseOnCluster(ctx, databaseName, clusterName)
	}
	return r.existsDatabaseSimple(ctx, databaseName)

}

func (r *DatabaseResource) existsDatabaseSimple(ctx context.Context, databaseName string) bool {
	var cnt uint64
	err := r.db.QueryRow(ctx, "SELECT count(1) FROM system.databases where `name` = ?", databaseName).Scan(&cnt)
	if err != nil {
		return false
	}
	if cnt != 1 {
		return false
	}
	return true
}

func (r *DatabaseResource) existsDatabaseOnCluster(ctx context.Context, databaseName string, clusterName string) bool {
	var hosts string
	var cnt_hosts int
	clusterQuery := `
	SELECT arrayStringConcat(groupArray(host_name), ',') as hosts, count(1) as cnt_hosts
	FROM system.clusters
	WHERE "cluster" = ?
	GROUP BY "cluster"`
	err := r.db.QueryRow(ctx, clusterQuery, clusterName).Scan(&hosts, &cnt_hosts)
	if err != nil {
		return false
	}
	var cnt int
	query := `SELECT count(1)
	FROM remote(?, system, "databases")
	WHERE "name" = ?`
	err = r.db.QueryRow(ctx, query, hosts, databaseName).Scan(&cnt)
	if err != nil {
		return false
	}
	if cnt != cnt_hosts {
		return false
	}
	return true
}
