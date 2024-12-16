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
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/katzucurry/terraform-provider-clickhouseops/internal/common"
)

var (
	_ resource.Resource                = &NamedCollection{}
	_ resource.ResourceWithConfigure   = &NamedCollection{}
	_ resource.ResourceWithImportState = &NamedCollection{}
)

func NewNamedCollection() resource.Resource {
	return &NamedCollection{}
}

type NamedCollection struct {
	db clickhouse.Conn
}

type NamedCollectionModel struct {
	ID                     types.String         `tfsdk:"id"`
	Name                   types.String         `tfsdk:"name"`
	ClusterName            types.String         `tfsdk:"cluster_name"`
	KeyValuePairs          []KeyValuePairsModel `tfsdk:"keyvaluepairs"`
	SensitiveKeyValuePairs []KeyValuePairsModel `tfsdk:"sensitive_keyvaluepairs"`
}

type KeyValuePairsModel struct {
	Key              types.String `tfsdk:"key"`
	Value            types.String `tfsdk:"value"`
	IsNotOverridable types.Bool   `tfsdk:"is_not_overridable"`
}

func (r *NamedCollection) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_namedcollection"
}

func (r *NamedCollection) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Clickhouse Named Collection to store secret",

		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Name Collection Name",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"cluster_name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Cluster Name",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"keyvaluepairs": schema.ListNestedAttribute{
				MarkdownDescription: "Clickhouse Named Collection Key-Value Pairs",
				Required:            true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"key": schema.StringAttribute{
							MarkdownDescription: "Clickhouse Named Collection Key",
							Required:            true,
						},
						"value": schema.StringAttribute{
							MarkdownDescription: "Clickhouse Named Collection Value",
							Required:            true,
						},
						"is_not_overridable": schema.BoolAttribute{
							MarkdownDescription: "Clickhouse Named Collection Disable Overridden",
							Optional:            true,
						},
					},
				},
				PlanModifiers: []planmodifier.List{
					listplanmodifier.RequiresReplace(),
				},
			},
			"sensitive_keyvaluepairs": schema.ListNestedAttribute{
				MarkdownDescription: "Clickhouse Named Collection Sensitive Key-Value Pairs",
				Optional:            true,
				Sensitive:           true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"key": schema.StringAttribute{
							MarkdownDescription: "Clickhouse Named Collection Key",
							Required:            true,
						},
						"value": schema.StringAttribute{
							MarkdownDescription: "Clickhouse Named Collection Value",
							Required:            true,
						},
						"is_not_overridable": schema.BoolAttribute{
							MarkdownDescription: "Clickhouse Named Collection Disable Overridden",
							Optional:            true,
						},
					},
				},
				PlanModifiers: []planmodifier.List{
					listplanmodifier.RequiresReplace(),
				},
			},
		},
	}
}

func (r *NamedCollection) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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
	Clickhouse NamedCollection Syntax for reference

CREATE NAMED COLLECTION [IF NOT EXISTS] name [ON CLUSTER cluster] AS
key_name1 = 'some value' [[NOT] OVERRIDABLE],
key_name2 = 'some value' [[NOT] OVERRIDABLE],
key_name3 = 'some value' [[NOT] OVERRIDABLE],
...
*/
const ddlCreateNamedCollectionTemplate = `
CREATE NAMED COLLECTION IF NOT EXISTS "{{.Name.ValueString}}"{{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}} AS
{{$size := size .KeyValuePairs}}
{{$size_sensitive := size .SensitiveKeyValuePairs}}
{{range $i, $e := .KeyValuePairs}}
"{{$e.Key.ValueString}}"='{{$e.Value.ValueString}}'{{if or (lt $i $size) (gt $size_sensitive -1)}},{{end}}
{{end}}
{{range $i, $e := .SensitiveKeyValuePairs}}
"{{$e.Key.ValueString}}"='{{$e.Value.ValueString}}'{{if lt $i $size_sensitive}},{{end}}
{{end}}
`

/*
DROP NAMED COLLECTION [IF EXISTS] name [on CLUSTER cluster]
.
*/
const ddlDropNamedCollectionTemplate = `
DROP NAMED COLLECTION IF EXISTS "{{.Name.ValueString}}"{{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}}
`

func (r *NamedCollection) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data *NamedCollectionModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	query, err := common.RenderTemplate(ddlCreateNamedCollectionTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse NamedCollection",
			"Could not render DDL, unexpected error: "+err.Error(),
		)
		return
	}

	err = r.db.Exec(ctx, *query)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse NamedCollection",
			"Could not render DDL, unexpected error: "+err.Error(),
		)
		return
	}

	data.ID = types.StringValue(data.ClusterName.ValueString() + ":" + data.Name.ValueString())

	tflog.Trace(ctx, "Created a NamedCollection Resource")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *NamedCollection) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data *NamedCollectionModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *NamedCollection) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data *NamedCollectionModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *NamedCollection) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data *NamedCollectionModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	query, err := common.RenderTemplate(ddlDropNamedCollectionTemplate, data)
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

func (r *NamedCollection) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
