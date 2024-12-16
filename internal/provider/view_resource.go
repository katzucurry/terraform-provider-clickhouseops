// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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
	_ resource.Resource                = &ViewResource{}
	_ resource.ResourceWithConfigure   = &ViewResource{}
	_ resource.ResourceWithImportState = &ViewResource{}
)

func NewViewResource() resource.Resource {
	return &ViewResource{}
}

type ViewResource struct {
	db *sql.DB
}

type ViewResourceModel struct {
	ID           types.String `tfsdk:"id"`
	Name         types.String `tfsdk:"name"`
	DatabaseName types.String `tfsdk:"database_name"`
	ClusterName  types.String `tfsdk:"cluster_name"`
	SQL          types.String `tfsdk:"sql"`
}

func (r *ViewResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_view"
}

func (r *ViewResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Clickhouse view",

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
			"database_name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse database name",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"cluster_name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse database name",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"sql": schema.StringAttribute{
				MarkdownDescription: "Clickhouse database comment",
				Required:            true,
			},
		},
	}
}

func (r *ViewResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

	r.db = db
}

func (r *ViewResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data *ViewResourceModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	queryTemplate := `CREATE OR REPLACE VIEW "{{.DatabaseName.ValueString}}"."{{.Name.ValueString}}" {{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}} AS {{.SQL.ValueString}}`
	query, err := common.RenderTemplate(queryTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse View",
			"Could not render DDL, unexpected error: "+err.Error(),
		)
		return
	}

	_, err = r.db.Exec(*query)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse View",
			"Could not execute DDL, unexpected error: "+err.Error(),
		)
		return
	}

	data.ID = types.StringValue(data.ClusterName.ValueString() + ":" + data.DatabaseName.ValueString() + ":" + data.Name.ValueString())

	tflog.Trace(ctx, "Created a View resource")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *ViewResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data *ViewResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if !data.ClusterName.IsNull() && !r.existViewInCluster(data.DatabaseName.ValueString(), data.Name.ValueString(), data.ClusterName.ValueString()) {
		tflog.Trace(ctx, "View doesn't exists in one or more Clickhouse nodes")
		resp.State.RemoveResource(ctx)
		return
	}
	as_select, err := r.readView(data.DatabaseName.ValueString(), data.Name.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("", ""+err.Error())
		return
	}

	formatedAsSelect, err := r.validateQuery(*as_select)
	if err != nil {
		tflog.Trace(ctx, "Could not validate SQL query stored in clickhouse, recreating")
		resp.State.RemoveResource(ctx)
		return
	}

	validateQuery, err := r.validateQuery(data.SQL.ValueString())
	if err != nil {
		tflog.Trace(ctx, "Could not validate SQL query stored in terraform state")
		resp.State.RemoveResource(ctx)
		return
	}

	if *formatedAsSelect != *validateQuery {
		data.SQL = types.StringValue(*formatedAsSelect)
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *ViewResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data *ViewResourceModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *ViewResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data *ViewResourceModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	queryTemplate := `DROP VIEW IF EXISTS "{{.DatabaseName.ValueString}}"."{{.Name.ValueString}}" {{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}}`
	query, err := common.RenderTemplate(queryTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError("", ""+err.Error())
		return
	}

	_, err = r.db.Exec(*query)
	if err != nil {
		resp.Diagnostics.AddError("", ""+err.Error())
		return
	}
}

func (r *ViewResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

type ReadViewResponse struct {
	UUID         string
	Name         string
	DatabaseName string
	SQL          string
}

func (r *ViewResource) readView(databaseName string, viewName string) (*string, error) {
	var as_select string
	err := r.db.QueryRow("SELECT as_select FROM system.`tables` WHERE `engine` = 'View' AND `database` = ? AND name = ?", databaseName, viewName).Scan(&as_select)
	if err != nil {
		return nil, err
	}
	return &as_select, nil
}

func (r *ViewResource) existViewInCluster(databaseName string, viewName string, clusterName string) bool {
	var hosts string
	var cnt_hosts int
	clusterQuery := `
	SELECT arrayStringConcat(groupArray(host_name), ',') as hosts, count(1) as cnt_hosts
	FROM system.clusters
	WHERE "cluster" = ?
	GROUP BY "cluster"`
	err := r.db.QueryRow(clusterQuery, clusterName).Scan(&hosts, &cnt_hosts)
	if err != nil {
		return false
	}
	var cnt int
	query := `SELECT count(1)
	FROM remote(?, system, "tables")
	WHERE "engine" = 'View' AND  "database" = ? AND "name" = ?`
	err = r.db.QueryRow(query, hosts, databaseName, viewName).Scan(&cnt)
	if err != nil {
		return false
	}
	if cnt != cnt_hosts {
		return false
	}
	return true
}

func (r *ViewResource) validateQuery(sql string) (*string, error) {
	validationQuery := "EXPLAIN SYNTAX " + sql
	results, err := r.db.Query(validationQuery)
	if err != nil {
		return nil, err
	}

	var parsedQuery []string
	for results.Next() {
		var explain string
		if err := results.Scan(&explain); err != nil {
			return nil, err
		}
		parsedQuery = append(parsedQuery, explain)
	}
	ret := strings.Join(parsedQuery, "\n") + "\n"
	return &ret, nil
}
