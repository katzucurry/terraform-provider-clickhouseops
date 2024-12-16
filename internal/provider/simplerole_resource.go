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
	_ resource.Resource                = &SimpleRole{}
	_ resource.ResourceWithConfigure   = &SimpleRole{}
	_ resource.ResourceWithImportState = &SimpleRole{}
)

func NewSimpleRole() resource.Resource {
	return &SimpleRole{}
}

type SimpleRole struct {
	db clickhouse.Conn
}

type SimpleRoleModel struct {
	ID          types.String `tfsdk:"id"`
	Name        types.String `tfsdk:"name"`
	ClusterName types.String `tfsdk:"cluster_name"`
}

func (r *SimpleRole) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_simplerole"
}

func (r *SimpleRole) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Clickhouse Simple Role",

		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				MarkdownDescription: "Name of the role",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"cluster_name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse cluster name",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
		},
	}
}

func (r *SimpleRole) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

/*
	Clickhouse SimpleRole Syntax for reference

CREATE ROLE [IF NOT EXISTS | OR REPLACE] name1 [ON CLUSTER cluster_name1] [, name2 [ON CLUSTER cluster_name2] ...]

	[IN access_storage_type]
	[SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
*/
const ddlSimpleRoleTemplate = `
CREATE ROLE '{{.Name.ValueString}}'{{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}} 
`

func (r *SimpleRole) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data *SimpleRoleModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	query, err := common.RenderTemplate(ddlSimpleRoleTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse SimpleRole",
			"Could not render DDL, unexpected error: "+err.Error(),
		)
		return
	}

	err = r.db.Exec(ctx, *query)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse SimpleRole",
			"Could not execute DDL, unexpected error: "+err.Error(),
		)
		return
	}

	data.ID = types.StringValue(data.ClusterName.ValueString() + ":" + data.Name.ValueString())

	tflog.Trace(ctx, "Created a SimpleRole Resource")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *SimpleRole) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data *SimpleRoleModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *SimpleRole) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data *SimpleRoleModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *SimpleRole) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data *SimpleRoleModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	queryTemplate := `DROP ROLE IF EXISTS '{{.Name.ValueString}}'{{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}}`
	query, err := common.RenderTemplate(queryTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError("", ""+err.Error())
		return
	}

	err = r.db.Exec(ctx, *query)
	if err != nil {
		resp.Diagnostics.AddError("", ""+err.Error())
		return
	}
}

func (r *SimpleRole) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
