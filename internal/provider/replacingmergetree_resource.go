package provider

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/awesomenessnil/terraform-provider-clickhouse/internal/common"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var (
	_ resource.Resource                = &ReplacingMergeTree{}
	_ resource.ResourceWithConfigure   = &ReplacingMergeTree{}
	_ resource.ResourceWithImportState = &ReplacingMergeTree{}
)

func NewReplacingMergeTree() resource.Resource {
	return &ReplacingMergeTree{}
}

type ReplacingMergeTree struct {
	db *sql.DB
}

type ReplacingMergeTreeModel struct {
	ID           types.String                      `tfsdk:"id"`
	Name         types.String                      `tfsdk:"name"`
	DatabaseName types.String                      `tfsdk:"database_name"`
	ClusterName  types.String                      `tfsdk:"cluster_name"`
	Columns      []ReplacingMergeTreeColumnsModel  `tfsdk:"columns"`
	IsReplicated types.Bool                        `tfsdk:"is_replicated"`
	Version      types.String                      `tfsdk:"version"`
	IsDeleted    types.String                      `tfsdk:"is_deleted"`
	PartitionBy  types.String                      `tfsdk:"partition_by"`
	OrderBy      []types.String                    `tfsdk:"order_by"`
	PrimaryKey   types.String                      `tfsdk:"primary_key"`
	SampleBy     types.String                      `tfsdk:"sample_by"`
	Settings     []ReplacingMergeTreeSettingsModel `tfsdk:"settings"`
}

type ReplacingMergeTreeColumnsModel struct {
	Name types.String `tfsdk:"name"`
	Type types.String `tfsdk:"type"`
}

type ReplacingMergeTreeSettingsModel struct {
	Name  types.String `tfsdk:"name"`
	Value types.String `tfsdk:"value"`
}

func (r *ReplacingMergeTree) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_replacingmergetree"
}

func (r *ReplacingMergeTree) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Clickhouse ReplacingMergeTree Table",

		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Table Name",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"database_name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Database Name",
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
			"columns": schema.ListNestedAttribute{
				MarkdownDescription: "Clickhouse Table Column List",
				Required:            true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							MarkdownDescription: "Clickhouse Table Column name",
							Required:            true,
						},
						"type": schema.
							StringAttribute{
							MarkdownDescription: "Clickhouse Table Column type",
							Required:            true,
						},
					},
				},
			},
			"is_replicated": schema.BoolAttribute{
				MarkdownDescription: "Clickhouse replicated ReplacingMergeTree",
				Optional:            true,
				// Default:             booldefault.StaticBool(false),
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.RequiresReplace(),
				},
			},
			"version": schema.StringAttribute{
				MarkdownDescription: "Column used to determine the version",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"is_deleted": schema.StringAttribute{
				MarkdownDescription: "Column use to determinate row is deleted",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"partition_by": schema.StringAttribute{
				MarkdownDescription: "ReplacingMergeTree column or expression for partitions",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"order_by": schema.ListAttribute{
				MarkdownDescription: "ReplacingMergeTree column or expression for order",
				ElementType:         types.StringType,
				Required:            true,
				PlanModifiers: []planmodifier.List{
					listplanmodifier.RequiresReplace(),
				},
			},
			"primary_key": schema.StringAttribute{
				MarkdownDescription: "ReplacingMergeTree column or expression for primary key",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"sample_by": schema.StringAttribute{
				MarkdownDescription: "ReplacingMergeTree column or expression for sample",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"settings": schema.ListNestedAttribute{
				MarkdownDescription: "ReplacingMergeTree optional settings",
				Optional:            true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							MarkdownDescription: "Clickhouse table setting name",
							Required:            true,
						},
						"value": schema.
							StringAttribute{
							MarkdownDescription: "Clickhouse table setting value",
							Required:            true,
						},
					},
				},
			},
		},
	}
}

func (r *ReplacingMergeTree) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

/* Clickhouse ReplacingMergeTree Syntax for reference
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver [, is_deleted]])
ORDER BY expr
[PARTITION BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, clean_deleted_rows=value, ...]
*/

const queryReplacingMergeTreeTemplate = `
CREATE TABLE "{{.DatabaseName.ValueString}}"."{{.Name.ValueString}}" {{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}} 
(
  {{range .Columns}}
  "{{.Name.ValueString}}" {{.Type.ValueString}},
  {{end}}
) ENGINE = {{if .IsReplicated.ValueBool}}ReplicatedReplacingMergeTree{{else}}ReplacingMergeTree{{end}}({{if not .Version.IsNull}}{{.Version.ValueString}}{{end}}{{if not .IsDeleted.IsNull}},{{.IsDeleted.ValueString}}{{end}})
{{if not .PartitionBy.IsNull}} PARTITION BY "{{.PartitionBy.ValueString}}"{{end}}
{{$size := size .OrderBy}}ORDER BY ({{range $i, $e := .OrderBy}}"{{$e.ValueString}}"{{if lt $i $size}},{{end}}{{end}})
{{if not .PrimaryKey.IsNull}} PRIMARY KEY "{{.PrimaryKey.ValueString}}"{{end}}
{{if not .SampleBy.IsNull}} SAMPLE BY "{{.SampleBy.ValueString}}"{{end}}
{{$size := size .Settings}}
{{with .Settings}}
SETTINGS
{{range $i, $e := .}}
{{.Name.ValueString}}='{{.Value.ValueString}}'{{if lt $i $size}},{{end}}
{{end}}
{{end}}
`

func (r *ReplacingMergeTree) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data *ReplacingMergeTreeModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	query, err := common.RenderTemplate(queryReplacingMergeTreeTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse ReplacingMergeTree Table",
			"Could not render DDL, unexpected error: "+err.Error(),
		)
		return
	}

	_, err = r.db.Exec(*query)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse ReplacingMergeTree Table",
			"Could not execute DDL, unexpected error: "+err.Error(),
		)
		return
	}

	data.ID = types.StringValue(data.ClusterName.ValueString() + ":" + data.DatabaseName.ValueString() + ":" + data.Name.ValueString())

	tflog.Trace(ctx, "Created a ReplacingMergeTree Table Resource")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *ReplacingMergeTree) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data *ReplacingMergeTreeModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *ReplacingMergeTree) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data *ReplacingMergeTreeModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *ReplacingMergeTree) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data *ReplacingMergeTreeModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	queryTemplate := `DROP TABLE IF EXISTS "{{.DatabaseName.ValueString}}"."{{.Name.ValueString}}" {{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}}`
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

func (r *ReplacingMergeTree) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
