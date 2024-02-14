package provider

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/awesomenessnil/terraform-provider-clickhouseops/internal/common"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var (
	_ resource.Resource                = &GrantAll{}
	_ resource.ResourceWithConfigure   = &GrantAll{}
	_ resource.ResourceWithImportState = &GrantAll{}
)

func NewGrantAll() resource.Resource {
	return &GrantAll{}
}

type GrantAll struct {
	db *sql.DB
}

type GrantAllModel struct {
	ID              types.String `tfsdk:"id"`
	DatabaseName    types.String `tfsdk:"database_name"`
	TableName       types.String `tfsdk:"table_name"`
	ClusterName     types.String `tfsdk:"cluster_name"`
	Assignee        types.String `tfsdk:"assignee"`
	WithGrantOption types.Bool   `tfsdk:"with_grant_option"`
}

func (r *GrantAll) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_grantall"
}

func (r *GrantAll) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Clickhouse grant select privilige to a user or a role (Assignee)",

		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"database_name": schema.StringAttribute{
				MarkdownDescription: "Name of the database where table you want to grant select permissions is located",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"table_name": schema.StringAttribute{
				MarkdownDescription: "Name of the table you want to grant select permissions",
				Optional:            true,
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
			"assignee": schema.StringAttribute{
				MarkdownDescription: "User or Role you want grant permissions",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"with_grant_option": schema.BoolAttribute{
				MarkdownDescription: "If true will assign grant option to the assignee",
				Optional:            true,
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.RequiresReplace(),
				},
			},
		},
	}
}

func (r *GrantAll) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

/*
	Clickhouse Grant Syntax for reference

GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION].
*/
const ddlCreateGrantAllTemplate = `
GRANT {{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}' {{end}}ALL ON {{if not .DatabaseName.IsNull}}"{{.DatabaseName.ValueString}}"{{else}}*{{end}}.{{if not .TableName.IsNull}}"{{.TableName.ValueString}}"{{else}}*{{end}} TO '{{.Assignee.ValueString}}' {{if .WithGrantOption.ValueBool}}WITH GRANT OPTION{{end}}
`

func (r *GrantAll) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data *GrantAllModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	query, err := common.RenderTemplate(ddlCreateGrantAllTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Granting Permissions",
			"Could not render DDL, unexpected error: "+err.Error(),
		)
		return
	}

	_, err = r.db.Exec(*query)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Granting Permissions",
			"Could not execute DDL, unexpected error: "+*query+err.Error(),
		)
		return
	}
	database := "*"
	if !data.DatabaseName.IsNull() {
		database = data.DatabaseName.ValueString()
	}
	table := "*"
	if !data.TableName.IsNull() {
		table = data.TableName.ValueString()
	}
	data.ID = types.StringValue(data.ClusterName.ValueString() + ":" + database + ":" + table + ":" + data.Assignee.ValueString())

	tflog.Trace(ctx, "Created a GrantAll Resource")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *GrantAll) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data *GrantAllModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *GrantAll) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data *GrantAllModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

const ddlDestroyGrantAllTemplate = `
REVOKE {{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}' {{end}}ALL ON {{if not .DatabaseName.IsNull}}"{{.DatabaseName.ValueString}}"{{else}}*{{end}}.{{if not .TableName.IsNull}}"{{.TableName.ValueString}}"{{else}}*{{end}} FROM '{{.Assignee.ValueString}}' {{if .WithGrantOption.ValueBool}}WITH GRANT OPTION{{end}}
`

func (r *GrantAll) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data *GrantAllModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	query, err := common.RenderTemplate(ddlDestroyGrantAllTemplate, data)
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

func (r *GrantAll) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
