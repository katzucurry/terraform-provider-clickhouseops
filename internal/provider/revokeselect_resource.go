package provider

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/awesomenessnil/terraform-provider-clickhouse/internal/common"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var (
	_ resource.Resource                = &RevokeSelect{}
	_ resource.ResourceWithConfigure   = &RevokeSelect{}
	_ resource.ResourceWithImportState = &RevokeSelect{}
)

func NewRevokeSelect() resource.Resource {
	return &RevokeSelect{}
}

type RevokeSelect struct {
	db *sql.DB
}

type RevokeSelectModel struct {
	ID           types.String   `tfsdk:"id"`
	DatabaseName types.String   `tfsdk:"database_name"`
	TableName    types.String   `tfsdk:"table_name"`
	ColumnsName  []types.String `tfsdk:"columns_name"`
	ClusterName  types.String   `tfsdk:"cluster_name"`
	Assignee     types.String   `tfsdk:"assignee"`
}

func (r *RevokeSelect) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_revokeselect"
}

func (r *RevokeSelect) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
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
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"table_name": schema.StringAttribute{
				MarkdownDescription: "Name of the table you want to grant select permissions",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"columns_name": schema.ListAttribute{
				MarkdownDescription: "List of columns is the user or role restricted on the target table",
				ElementType:         types.StringType,
				Optional:            true,
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
		},
	}
}

func (r *RevokeSelect) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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
const ddlCreateRevokeSelectTemplate = `
REVOKE {{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}' {{end}}SELECT{{$size := size .ColumnsName}}{{with .ColumnsName}}({{range $i, $e := .}}"{{$e.ValueString}}"{{if lt $i $size}},{{end}}{{end}}){{end}} ON "{{.DatabaseName.ValueString}}"."{{.TableName.ValueString}}" FROM '{{.Assignee.ValueString}}'
`

func (r *RevokeSelect) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data *RevokeSelectModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	query, err := common.RenderTemplate(ddlCreateRevokeSelectTemplate, data)
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
	var columns []string
	for _, v := range data.ColumnsName {
		columns = append(columns, v.ValueString())
	}
	data.ID = types.StringValue(data.ClusterName.ValueString() + ":" + strings.Join(columns, ":") + ":" + data.Assignee.ValueString())

	tflog.Trace(ctx, "Created a RevokeSelect Resource")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *RevokeSelect) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data *RevokeSelectModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *RevokeSelect) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data *RevokeSelectModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

const ddlDestroyRevokeSelectTemplate = `
GRANT {{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}' {{end}}SELECT{{$size := size .ColumnsName}}{{with .ColumnsName}}({{range $i, $e := .}}"{{$e.ValueString}}"{{if lt $i $size}},{{end}}{{end}}){{end}} ON "{{.DatabaseName.ValueString}}"."{{.TableName.ValueString}}" TO '{{.Assignee.ValueString}}'
`

func (r *RevokeSelect) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data *RevokeSelectModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	query, err := common.RenderTemplate(ddlDestroyRevokeSelectTemplate, data)
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

func (r *RevokeSelect) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
