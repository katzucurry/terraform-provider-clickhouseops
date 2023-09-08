// CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
// (
//     name1 [type1] [ALIAS expr1],
//     name2 [type2] [ALIAS expr2],
//     ...
// ) ENGINE = Kafka()
// SETTINGS
//     kafka_broker_list = 'host:port',
//     kafka_topic_list = 'topic1,topic2,...',
//     kafka_group_name = 'group_name',
//     kafka_format = 'data_format'[,]
//     [kafka_row_delimiter = 'delimiter_symbol',]
//     [kafka_schema = '',]
//     [kafka_num_consumers = N,]
//     [kafka_max_block_size = 0,]
//     [kafka_skip_broken_messages = N,]
//     [kafka_commit_every_batch = 0,]
//     [kafka_client_id = '',]
//     [kafka_poll_timeout_ms = 0,]
//     [kafka_poll_max_batch_size = 0,]
//     [kafka_flush_interval_ms = 0,]
//     [kafka_thread_per_consumer = 0,]
//     [kafka_handle_error_mode = 'default',]
//     [kafka_commit_on_select = false,]
//     [kafka_max_rows_per_message = 1];

package provider

import (
	"context"
	"database/sql"
	"fmt"

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
	_ resource.Resource                = &KafkaEngineResource{}
	_ resource.ResourceWithConfigure   = &KafkaEngineResource{}
	_ resource.ResourceWithImportState = &KafkaEngineResource{}
)

func NewKafkaEngineResource() resource.Resource {
	return &KafkaEngineResource{}
}

type KafkaEngineResource struct {
	db *sql.DB
}

type KafkaEngineResourceModel struct {
	ID                types.String              `tfsdk:"id"`
	Name              types.String              `tfsdk:"name"`
	DatabaseName      types.String              `tfsdk:"database_name"`
	ClusterName       types.String              `tfsdk:"cluster_name"`
	Columns           []KafkaEngineColumnsModel `tfsdk:"columns"`
	BrokerList        types.String              `tfsdk:"kafka_broker_list"`
	TopicList         types.String              `tfsdk:"kafka_topic_list"`
	GroupName         types.String              `tfsdk:"kafka_group_name"`
	Format            types.String              `tfsdk:"kafka_format"`
	SchemaRegistryURL types.String              `tfsdk:"format_avro_schema_registry_url"`
}

type KafkaEngineColumnsModel struct {
	Name types.String `tfsdk:"name"`
	Type types.String `tfsdk:"type"`
}

func (r *KafkaEngineResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_kafkaengine"
}

func (r *KafkaEngineResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Clickhouse KafkaEngine Table",

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
			"kafka_broker_list": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Kafka Broker List",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"kafka_topic_list": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Kafka Topic List",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"kafka_group_name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Kafka Group Name",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"kafka_format": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Kafka Format",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"format_avro_schema_registry_url": schema.StringAttribute{
				MarkdownDescription: "Clickhouse Format Avro Schema Registry URL",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
		},
	}
}

func (r *KafkaEngineResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *KafkaEngineResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data *KafkaEngineResourceModel

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
	) ENGINE = Kafka()
	SETTINGS
	    kafka_broker_list = '{{.BrokerList.ValueString}}',
	    kafka_topic_list = '{{.TopicList.ValueString}}',
	    kafka_group_name = '{{.GroupName.ValueString}}',
	    kafka_format = '{{.Format.ValueString}}'{{if not .SchemaRegistryURL.IsNull}},{{end}} 
		{{if not .SchemaRegistryURL.IsNull}} format_avro_schema_registry_url = '{{.SchemaRegistryURL.ValueString}}'{{end}}
	`
	query, err := common.RenderTemplate(queryTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse KafkaEngine Table",
			"Could not render DDL, unexpected error: "+err.Error(),
		)
		return
	}

	_, err = r.db.Exec(*query)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse KafkaEngine Table",
			"Could not execute DDL, unexpected error: "+err.Error(),
		)
		return
	}

	data.ID = types.StringValue(data.ClusterName.ValueString() + ":" + data.DatabaseName.ValueString() + ":" + data.Name.ValueString())

	tflog.Trace(ctx, "Created a KafkaEngine Table Resource")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *KafkaEngineResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data *KafkaEngineResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *KafkaEngineResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data *KafkaEngineResourceModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *KafkaEngineResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data *KafkaEngineResourceModel

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

func (r *KafkaEngineResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
