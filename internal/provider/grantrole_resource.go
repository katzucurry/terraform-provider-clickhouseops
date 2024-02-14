package provider

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/awesomenessnil/terraform-provider-clickhouseops/internal/common"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var (
	_ resource.Resource                = &GrantRole{}
	_ resource.ResourceWithConfigure   = &GrantRole{}
	_ resource.ResourceWithImportState = &GrantRole{}
)

func NewGrantRole() resource.Resource {
	return &GrantRole{}
}

type GrantRole struct {
	db *sql.DB
}

type GrantRoleModel struct {
	ID          types.String `tfsdk:"id"`
	RoleName    types.String `tfsdk:"role_name"`
	UserName    types.String `tfsdk:"user_name"`
	ClusterName types.String `tfsdk:"cluster_name"`
}

func (r *GrantRole) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_grantrole"
}

func (r *GrantRole) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Clickhouse assign role to a user",

		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"role_name": schema.StringAttribute{
				MarkdownDescription: "Role name to assign to the user",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"user_name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse username",
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

func (r *GrantRole) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION] [WITH REPLACE OPTION].
*/
const ddlCreateGrantRoleTemplate = `GRANT {{if not .ClusterName.IsNull}}ON CLUSTER '{{.ClusterName.ValueString}}' {{end}}'{{.RoleName.ValueString}}' TO '{{.UserName.ValueString}}'`

func (r *GrantRole) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data *GrantRoleModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	query, err := common.RenderTemplate(ddlCreateGrantRoleTemplate, data)
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
	data.ID = types.StringValue(data.ClusterName.ValueString() + ":" + data.RoleName.ValueString() + ":" + data.UserName.ValueString())

	tflog.Trace(ctx, "Created a GrantRole Resource")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *GrantRole) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data *GrantRoleModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *GrantRole) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data *GrantRoleModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

const ddlDestroyGrantRoleTemplate = `REVOKE {{if not .ClusterName.IsNull}}ON CLUSTER '{{.ClusterName.ValueString}}' {{end}}'{{.RoleName.ValueString}}' FROM '{{.UserName.ValueString}}'`

func (r *GrantRole) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data *GrantRoleModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	query, err := common.RenderTemplate(ddlDestroyGrantRoleTemplate, data)
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

func (r *GrantRole) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
