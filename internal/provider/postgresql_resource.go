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
	_ resource.Resource                   = &PostgreSQL{}
	_ resource.ResourceWithConfigure      = &PostgreSQL{}
	_ resource.ResourceWithImportState    = &PostgreSQL{}
	_ resource.ResourceWithValidateConfig = &PostgreSQL{}
)

func NewPostgreSQL() resource.Resource {
	return &PostgreSQL{}
}

type PostgreSQL struct {
	db *sql.DB
}

type PostgreSQLModel struct {
	ID                     types.String             `tfsdk:"id"`
	Name                   types.String             `tfsdk:"name"`
	DatabaseName           types.String             `tfsdk:"database_name"`
	ClusterName            types.String             `tfsdk:"cluster_name"`
	Columns                []PostgreSQLColumnsModel `tfsdk:"columns"`
	NamedCollectionName    types.String             `tfsdk:"named_collection_name"`
	PostgreSQLHost         types.String             `tfsdk:"postgresql_host"`
	PostgreSQLPort         types.String             `tfsdk:"postgresql_port"`
	PostgreSQLDatabaseName types.String             `tfsdk:"postgresql_database_name"`
	PostgreSQLTableName    types.String             `tfsdk:"postgresql_table_name"`
	PostgreSQLUsername     types.String             `tfsdk:"postgresql_username"`
	PostgreSQLPassword     types.String             `tfsdk:"postgresql_password"`
	PostgreSQLSchema       types.String             `tfsdk:"postgresql_schema"`
}

type PostgreSQLColumnsModel struct {
	Name types.String `tfsdk:"name"`
	Type types.String `tfsdk:"type"`
}

func (r *PostgreSQL) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_postgresql"
}

func (r *PostgreSQL) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Clickhouse materialized view",

		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse PostgreSQL name",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"database_name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse PostgreSQL database name",
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
			"columns": schema.ListNestedAttribute{
				MarkdownDescription: "Clickhouse PostgreSQL column list",
				Required:            true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							MarkdownDescription: "Clickhouse table column name",
							Required:            true,
						},
						"type": schema.
							StringAttribute{
							MarkdownDescription: "Clickhouse table column type",
							Required:            true,
						},
					},
				},
			},
			"named_collection_name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse NamedCollection with PostgreSQL connection configuration",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"postgresql_host": schema.StringAttribute{
				MarkdownDescription: "Clickhouse PostgreSQL connection host",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"postgresql_port": schema.StringAttribute{
				MarkdownDescription: "Clickhouse PostgreSQL connection port",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"postgresql_database_name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse PostgreSQL connection database name",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"postgresql_table_name": schema.StringAttribute{
				MarkdownDescription: "Clickhouse PostgreSQL connection table name",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"postgresql_username": schema.StringAttribute{
				MarkdownDescription: "Clickhouse PostgreSQL connection username",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"postgresql_password": schema.StringAttribute{
				MarkdownDescription: "Clickhouse PostgreSQL connection password",
				Optional:            true,
				Sensitive:           true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"postgresql_schema": schema.StringAttribute{
				MarkdownDescription: "Clickhouse PostgreSQL connection schema",
				Optional:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
		},
	}
}

func (r *PostgreSQL) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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
	Clickhouse PostgreSQL Syntax for reference

CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(

	name1 type1 [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
	name2 type2 [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
	...

) ENGINE = PostgreSQL('host:port', 'database', 'table', 'user', 'password'[, `schema`])
.
*/
const ddlCreatePostgreSQLTemplate = `
CREATE TABLE IF NOT EXISTS "{{.DatabaseName.ValueString}}"."{{.Name.ValueString}}"{{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}} (
{{range .Columns}}
"{{.Name.ValueString}}" {{.Type.ValueString}},
{{end}}
) ENGINE = PostgreSQL(
{{if not .NamedCollectionName.IsNull}}{{.NamedCollectionName.ValueString}}
{{if not .PostgreSQLHost.IsNull}},host={{.PostgreSQLHost.ValueString}}{{end}}
{{if not .PostgreSQLPort.IsNull}},port={{.PostgreSQLPort.ValueString}}{{end}}
{{if not .PostgreSQLDatabaseName.IsNull}},database={{.PostgreSQLDatabaseName.ValueString}}{{end}}
{{if not .PostgreSQLTableName.IsNull}},table={{.PostgreSQLTableName.ValueString}}{{end}}
{{if not .PostgreSQLUsername.IsNull}},user=.PostgreSQLUsername.ValueString{{end}}
{{if not .PostgreSQLPassword.IsNull}}password=,{{.PostgreSQLPassword.ValueString}}{{end}}
{{if not .PostgreSQLSchema.IsNull}},schema={{.PostgreSQLSchema.ValueString}}{{end}})
{{else}}
{{if not .PostgreSQLHost.IsNull}}'{{.PostgreSQLHost.ValueString}}{{end}}{{if not .PostgreSQLPort.IsNull}}:{{.PostgreSQLPort.ValueString}}{{end}}'
{{if not .PostgreSQLDatabaseName.IsNull}}, '{{.PostgreSQLDatabaseName.ValueString}}'{{end}}
{{if not .PostgreSQLTableName.IsNull}}, '{{.PostgreSQLTableName.ValueString}}'{{end}}
{{if not .PostgreSQLUsername.IsNull}}, '{{.PostgreSQLUsername.ValueString}}'{{end}}
{{if not .PostgreSQLPassword.IsNull}}, '{{.PostgreSQLPassword.ValueString}}'{{end}}
{{if not .PostgreSQLSchema.IsNull}}, '{{.PostgreSQLSchema.ValueString}}'{{end}})
{{end}}
`

/*
DROP [TEMPORARY] TABLE [IF EXISTS] [IF EMPTY] [db.]name [ON CLUSTER cluster] [SYNC]
.
*/
const ddlDropPostgreSQLTemplate = `
DROP TABLE IF EXISTS "{{.DatabaseName.ValueString}}"."{{.Name.ValueString}}" {{if not .ClusterName.IsNull}} ON CLUSTER '{{.ClusterName.ValueString}}'{{end}}
`

func (r *PostgreSQL) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data *PostgreSQLModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	query, err := common.RenderTemplate(ddlCreatePostgreSQLTemplate, data)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse PostgreSQL",
			"Could not render DDL, unexpected error: "+err.Error(),
		)
		return
	}

	_, err = r.db.Exec(*query)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Clickhouse PostgreSQL",
			"Could not execute DDL: "+*query+", unexpected error: "+err.Error(),
		)
		return
	}

	data.ID = types.StringValue(data.ClusterName.ValueString() + ":" + data.Name.ValueString())

	tflog.Trace(ctx, "Created a PostgreSQL Resource")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *PostgreSQL) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data *PostgreSQLModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *PostgreSQL) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data *PostgreSQLModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *PostgreSQL) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data *PostgreSQLModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	query, err := common.RenderTemplate(ddlDropPostgreSQLTemplate, data)
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

func (r *PostgreSQL) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

func (r *PostgreSQL) ValidateConfig(ctx context.Context, req resource.ValidateConfigRequest, resp *resource.ValidateConfigResponse) {
	var data *PostgreSQLModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	if !data.NamedCollectionName.IsNull() {
		return
	}

	if !data.PostgreSQLHost.IsNull() &&
		!data.PostgreSQLPort.IsNull() &&
		!data.PostgreSQLDatabaseName.IsNull() &&
		!data.PostgreSQLTableName.IsNull() &&
		!data.PostgreSQLUsername.IsNull() &&
		!data.PostgreSQLPassword.IsNull() {
		return
	}

	var errorPath path.Path

	if data.PostgreSQLPassword.IsNull() {
		errorPath = path.Root("postgresql_password")
	}
	if data.PostgreSQLUsername.IsNull() {
		errorPath = path.Root("postgresql_username")
	}
	if data.PostgreSQLTableName.IsNull() {
		errorPath = path.Root("postgresql_table_name")
	}
	if data.PostgreSQLDatabaseName.IsNull() {
		errorPath = path.Root("postgresql_database_name")
	}
	if data.PostgreSQLPort.IsNull() {
		errorPath = path.Root("postgresql_port")
	}
	if data.PostgreSQLHost.IsNull() {
		errorPath = path.Root("postgresql_host")
	}

	resp.Diagnostics.AddAttributeError(
		errorPath,
		"Missing Attribute Configuration",
		"Expect a Clickhouse named collection or the complete set of PostgreSQL connection parameters",
	)
}
