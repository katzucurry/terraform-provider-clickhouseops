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
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var (
	_ resource.Resource                = &MergeTreeResource{}
	_ resource.ResourceWithConfigure   = &MergeTreeResource{}
	_ resource.ResourceWithImportState = &MergeTreeResource{}
)

func NewMergeTreeResource() resource.Resource {
	return &MergeTreeResource{}
}

type MergeTreeResource struct {
	db *sql.DB
}

type MergeTreeResourceModel struct {
	ID           types.String             `tfsdk:"id"`
	Name         types.String             `tfsdk:"name"`
	DatabaseName types.String             `tfsdk:"database_name"`
	ClusterName  types.String             `tfsdk:"cluster_name"`
	Columns      []MergeTreeColumnsModel  `tfsdk:"columns"`
	IsReplicated types.Bool               `tfsdk:"is_replicated"`
	OrderBy      []types.String           `tfsdk:"order_by"`
	PartitionBy  types.String             `tfsdk:"partition_by"`
	PrimaryKey   types.String             `tfsdk:"primary_key"`
	Settings     []MergeTreeSettingsModel `tfsdk:"settings"`
}

type MergeTreeColumnsModel struct {
	Name types.String `tfsdk:"name"`
	Type types.String `tfsdk:"type"`
}

type MergeTreeSettingsModel struct {
	Name  types.String `tfsdk:"name"`
	Value types.String `tfsdk:"value"`
}

func (r *MergeTreeResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_mergetree"
}

func (r *MergeTreeResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Clickhouse MergeTree Table",

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
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.RequiresReplace(),
				},
			},
			"order_by": schema.ListAttribute{
				MarkdownDescription: "Clickhous columns list for order by",
				ElementType:         types.StringType,
				Required:            true,
				PlanModifiers: []planmodifier.List{
					listplanmodifier.RequiresReplace(),
				},
			},
			"partition_by": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Cluster Name",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"primary_key": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Cluster Name",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"settings": schema.ListNestedAttribute{
				MarkdownDescription: "MergeTree optional settings",
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

func (r *MergeTreeResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *MergeTreeResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data *MergeTreeResourceModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	queryTemplate := `
	CREATE TABLE "{{.DatabaseName.ValueString}}"."{{.Name.ValueString}}" {{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}} 
	(
		{{range .Columns}}
		{{.Name.ValueString}} {{.Type.ValueString}},
		{{end}}
	) ENGINE = {{if .IsReplicated.ValueBool}}ReplicatedMergeTree(){{else}}MergeTree(){{end}}
	{{$size := size .OrderBy}}ORDER BY ({{range $i, $e := .OrderBy}}"{{$e.ValueString}}"{{if lt $i $size}},{{end}}{{end}})
	{{if not .PartitionBy.IsNull}}PARTITION BY {{.PartitionBy.ValueString}}{{end}}	
	{{if not .PrimaryKey.IsNull}}PRIMARY KEY {{.PrimaryKey.ValueString}}{{end}}
	{{$size := size .Settings}}
	{{with .Settings}}
	SETTINGS
	{{range $i, $e := .}}
	{{.Name.ValueString}}='{{.Value.ValueString}}'{{if lt $i $size}},{{end}}
	{{end}}
	{{end}}
	`
	query, err := common.RenderTemplate(queryTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse MergeTree Table",
			"Could not render DDL, unexpected error: "+err.Error(),
		)
		return
	}

	_, err = r.db.Exec(*query)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse MergeTree Table",
			"Could not execute DDL, unexpected error: "+err.Error(),
		)
		return
	}

	data.ID = types.StringValue(data.ClusterName.ValueString() + ":" + data.DatabaseName.ValueString() + ":" + data.Name.ValueString())

	tflog.Trace(ctx, "Created a MergeTree Table Resource")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *MergeTreeResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data *MergeTreeResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *MergeTreeResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data *MergeTreeResourceModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *MergeTreeResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data *MergeTreeResourceModel

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

func (r *MergeTreeResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
