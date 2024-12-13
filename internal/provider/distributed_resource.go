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
	_ resource.Resource                = &Distributed{}
	_ resource.ResourceWithConfigure   = &Distributed{}
	_ resource.ResourceWithImportState = &Distributed{}
)

func NewDistributed() resource.Resource {
	return &Distributed{}
}

type Distributed struct {
	db *sql.DB
}

type DistributedModel struct {
	ID              types.String               `tfsdk:"id"`
	Name            types.String               `tfsdk:"name"`
	DatabaseName    types.String               `tfsdk:"database_name"`
	ClusterName     types.String               `tfsdk:"cluster_name"`
	Columns         []DistributedColumnsModel  `tfsdk:"columns"`
	DistCluster     types.String               `tfsdk:"dist_cluster"`
	DistDatabase    types.String               `tfsdk:"dist_database"`
	DistTable       types.String               `tfsdk:"dist_table"`
	DistShardingKey types.String               `tfsdk:"dist_sharding_key"`
	Settings        []DistributedSettingsModel `tfsdk:"settings"`
}

type DistributedColumnsModel struct {
	Name types.String `tfsdk:"name"`
	Type types.String `tfsdk:"type"`
}

type DistributedSettingsModel struct {
	Name  types.String `tfsdk:"name"`
	Value types.String `tfsdk:"value"`
}

// Metadata implements resource.Resource.
func (d *Distributed) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_distributed"
}

// Schema implements resource.Resource.
func (d *Distributed) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Clickhouse Distibuted Table",

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
			"dist_cluster": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Cluster Name",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"dist_database": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Database Name",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"dist_table": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Table Name",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"dist_sharding_key": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Sharding Key",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"settings": schema.ListNestedAttribute{
				MarkdownDescription: "Distributed optional settings",
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

// Configure implements resource.ResourceWithConfigure.
func (d *Distributed) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

/* Clickhouse Distributed syntax
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]])
[SETTINGS name=value, ...]
*/

const distributedTemplate = `
CREATE TABLE "{{.DatabaseName.ValueString}}"."{{.Name.ValueString}}" {{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}} 
(
  {{- range .Columns}}
  "{{.Name.ValueString}}" {{.Type.ValueString}},
  {{- end}}
) ENGINE = Distributed(
'{{- .DistCluster.ValueString }}',
'{{- .DistDatabase.ValueString }}',
'{{- .DistTable.ValueString }}'
{{- if not .DistShardingKey.IsNull}}
,{{.DistShardingKey.ValueString}}
{{- end }})
{{- $size := size .Settings }}
{{- with .Settings }}
SETTINGS
{{- range $i, $e := . }}
{{.Name.ValueString}}='{{.Value.ValueString}}'{{if lt $i $size}},{{end}}
{{- end}}
{{- end}}
`

// Create implements resource.Resource.
func (d *Distributed) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data *DistributedModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	query, err := common.RenderTemplate(distributedTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse Distributed Table",
			"Could not render DDL, unexpected error: "+err.Error(),
		)
		return
	}

	tflog.Info(ctx, *query)

	_, err = d.db.Exec(*query)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse Distributed Table",
			"Could not execute DDL, unexpected error: "+err.Error()+*query,
		)
		return
	}

	data.ID = types.StringValue(data.ClusterName.ValueString() + ":" + data.DatabaseName.ValueString() + ":" + data.Name.ValueString())

	tflog.Trace(ctx, "Created a Distributed Table Resource")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (d *Distributed) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data *DistributedModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (d *Distributed) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data *DistributedModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

// Delete implements resource.Resource.
func (d *Distributed) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data *DistributedModel

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

	_, err = d.db.Exec(*query)
	if err != nil {
		resp.Diagnostics.AddError("", ""+err.Error())
		return
	}
}

// ImportState implements resource.ResourceWithImportState.
func (d *Distributed) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
