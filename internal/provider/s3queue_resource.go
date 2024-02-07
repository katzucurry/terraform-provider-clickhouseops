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
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var (
	_ resource.Resource                = &S3Queue{}
	_ resource.ResourceWithConfigure   = &S3Queue{}
	_ resource.ResourceWithImportState = &S3Queue{}
)

func NewS3Queue() resource.Resource {
	return &S3Queue{}
}

type S3Queue struct {
	db *sql.DB
}

type S3QueueModel struct {
	ID                  types.String           `tfsdk:"id"`
	Name                types.String           `tfsdk:"name"`
	DatabaseName        types.String           `tfsdk:"database_name"`
	ClusterName         types.String           `tfsdk:"cluster_name"`
	Columns             []S3QueueColumnsModel  `tfsdk:"columns"`
	NamedCollectionName types.String           `tfsdk:"named_collection_name"`
	Path                types.String           `tfsdk:"path"`
	NoSign              types.Bool             `tfsdk:"nosign"`
	AwsAccessKeyId      types.String           `tfsdk:"aws_access_key_id"`
	AwsSecretAccessKey  types.String           `tfsdk:"aws_secret_access_key"`
	Format              types.String           `tfsdk:"format"`
	Compression         types.String           `tfsdk:"compression"`
	Settings            []S3QueueSettingsModel `tfsdk:"settings"`
}

type S3QueueColumnsModel struct {
	Name types.String `tfsdk:"name"`
	Type types.String `tfsdk:"type"`
}

type S3QueueSettingsModel struct {
	Name  types.String `tfsdk:"name"`
	Value types.String `tfsdk:"value"`
}

func (r *S3Queue) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_s3queue"
}

func (r *S3Queue) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Clickhouse S3Queue Table",

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
			"named_collection_name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Named Collection containing S3Queue config",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"path": schema.StringAttribute{
				MarkdownDescription: "S3 Path where data are stored",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"nosign": schema.BoolAttribute{
				MarkdownDescription: "S3 config to sign api requests",
				Optional:            true,
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.RequiresReplace(),
				},
			},
			"aws_access_key_id": schema.StringAttribute{
				MarkdownDescription: "aws_access_key_id to access s3 bucket",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"aws_secret_access_key": schema.StringAttribute{
				MarkdownDescription: "aws_secret_access_key to access s3 bucket3",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"format": schema.StringAttribute{
				MarkdownDescription: "S3Queue format config",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"compression": schema.StringAttribute{
				MarkdownDescription: "S3Queue compression config",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"settings": schema.ListNestedAttribute{
				MarkdownDescription: "S3Queue optional settings",
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

func (r *S3Queue) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

/* Clickhouse S3Queue Syntax for reference
CREATE TABLE s3_queue_engine_table (name String, value UInt32)
    ENGINE = S3Queue(path, [NOSIGN, | aws_access_key_id, aws_secret_access_key,] format, [compression])
    [SETTINGS]
    [mode = 'unordered',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    [s3queue_loading_retries = 0,]
    [s3queue_processing_threads_num = 1,]
    [s3queue_enable_logging_to_s3queue_log = 0,]
    [s3queue_polling_min_timeout_ms = 1000,]
    [s3queue_polling_max_timeout_ms = 10000,]
    [s3queue_polling_backoff_ms = 0,]
    [s3queue_tracked_file_ttl_sec = 0,]
    [s3queue_tracked_files_limit = 1000,]
    [s3queue_cleanup_interval_min_ms = 10000,]
    [s3queue_cleanup_interval_max_ms = 30000,]
*/

const queryS3QueueTemplate = `
CREATE TABLE "{{.DatabaseName.ValueString}}"."{{.Name.ValueString}}" {{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}} 
(
  {{range .Columns}}
  "{{.Name.ValueString}}" {{.Type.ValueString}},
  {{end}}
) ENGINE = S3Queue(
{{if not .NamedCollectionName.IsNull}}{{.NamedCollectionName.ValueString}}
{{if not .Path.IsNull}},path='{{.Path.ValueString}}'{{end}}
{{if not .NoSign.IsNull}}{{if .NoSign.ValueBool}},NOSIGN='NOSIGN'{{end}}
{{else}}
{{if not .AwsAccessKeyId.IsNull}},aws_access_key_id='{{.AwsAccessKeyId.ValueString}}'{{end}}
{{if not .AwsSecretAccessKey.IsNull}},aws_secret_access_key='{{.AwsSecretAccessKey.ValueString}}'{{end}}
{{end}}
{{if not .Format.IsNull}}, format='{{.Format.ValueString}}'{{end}}
{{if not .Compression.IsNull}},compression='{{.Compression.ValueString}}'{{end}}
{{else}}
'{{.Path.ValueString}}'
{{if not .NoSign.IsNull}}{{if .NoSign.ValueBool}},NOSIGN{{end}}
{{else}}
,'{{.AwsAccessKeyId.ValueString}}'
,'{{.AwsSecretAccessKey.ValueString}}'
{{end}}
,'{{.Format.ValueString}}'
{{if not .Compression.IsNull}},'{{.Compression.ValueString}}'{{end}}
{{end}})
{{$size := size .Settings}}
{{with .Settings}}
SETTINGS
{{range $i, $e := .}}
{{.Name.ValueString}}='{{.Value.ValueString}}'{{if lt $i $size}},{{end}}
{{end}}
{{end}}
`

func (r *S3Queue) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data *S3QueueModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if data.NamedCollectionName.IsNull() && data.Path.IsNull() {
		resp.Diagnostics.AddError(
			"Missing Attribute Configuration",
			"Expect a Clickhouse named collection or the complete set of S3Queue parameters configuration",
		)
		return
	}

	query, err := common.RenderTemplate(queryS3QueueTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse S3Queue Table",
			"Could not render DDL, unexpected error: "+err.Error(),
		)
		return
	}

	_, err = r.db.Exec(*query)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse S3Queue Table",
			"Could not execute DDL, unexpected error: "+err.Error(),
		)
		return
	}

	data.ID = types.StringValue(data.ClusterName.ValueString() + ":" + data.DatabaseName.ValueString() + ":" + data.Name.ValueString())

	tflog.Trace(ctx, "Created a S3Queue Table Resource")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *S3Queue) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data *S3QueueModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *S3Queue) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data *S3QueueModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *S3Queue) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data *S3QueueModel

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

func (r *S3Queue) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
