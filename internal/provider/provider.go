package provider

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var _ provider.Provider = &ClickhouseProvider{}

const (
	host     = "localhost"
	port     = "9000"
	username = "default"
	password = ""
)

type ClickhouseProvider struct {
	// version is set to the provider version on release, "dev" when the
	// provider is built and ran locally, and "test" when running acceptance
	// testing.
	version string
}

type ClickhouseProviderModel struct {
	Host     types.String `tfsdk:"host"`
	Port     types.Int64  `tfsdk:"port"`
	Username types.String `tfsdk:"username"`
	Password types.String `tfsdk:"password"`
	Secure   types.Bool   `tfsdk:"secure"`
}

func (p *ClickhouseProvider) Metadata(ctx context.Context, req provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "clickhouseops"
	resp.Version = p.version
}

func (p *ClickhouseProvider) Schema(ctx context.Context, req provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"host": schema.StringAttribute{
				MarkdownDescription: "Clickhouse server host",
				Optional:            true,
			},
			"port": schema.Int64Attribute{
				MarkdownDescription: "Clichhouse server port",
				Optional:            true,
			},
			"username": schema.StringAttribute{
				MarkdownDescription: "Clickhouse server valid username",
				Optional:            true,
			},
			"password": schema.StringAttribute{
				MarkdownDescription: "Clickhouse server password",
				Optional:            true,
				Sensitive:           true,
			},
			"secure": schema.BoolAttribute{
				MarkdownDescription: "Clickhouse secure connection using SSL/TLS",
				Optional:            true,
			},
		},
	}
}

func (p *ClickhouseProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var data ClickhouseProviderModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	host := getEnv("TF_CH_HOST", host)
	port, err := strconv.ParseInt(getEnv("TF_CH_PORT", port), 10, 64)
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Create Clickhouse Client",
			"An unexpected error occurred when parsing port number"+
				"Clickhouse Client Error: "+err.Error(),
		)
		return
	}
	username := getEnv("TF_CH_USERNMAE", username)
	password := getEnv("TF_CH_PASSWORD", password)
	var secureConfig *tls.Config

	if !data.Host.IsNull() {
		host = data.Host.ValueString()
	}

	if !data.Port.IsNull() {
		port = data.Port.ValueInt64()
	}

	if !data.Username.IsNull() {
		username = data.Username.ValueString()
	}

	if !data.Password.IsNull() {
		password = data.Password.ValueString()
	}

	if !data.Secure.IsNull() && data.Secure.ValueBool() {
		secureConfig = &tls.Config{InsecureSkipVerify: false}
	}

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Username: username,
			Password: password,
		},
		TLS: secureConfig,
	})

	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Create Clickhouse Client",
			"An unexpected error occurred when open a connection"+
				"Clickhouse Client Error: "+err.Error(),
		)
		return
	}
	resp.DataSourceData = db
	resp.ResourceData = db
}

func (p *ClickhouseProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewDatabaseResource,
		NewViewResource,
		NewMergeTreeResource,
		NewKafkaEngineResource,
		NewReplacingMergeTree,
		NewNamedCollection,
		NewMaterializedView,
		NewPostgreSQL,
		NewS3Queue,
		NewSimpleUser,
		NewSimpleRole,
		NewGrantSelect,
		NewGrantRole,
		NewRevokeSelect,
		NewGrantAll,
		NewDistributed,
	}
}

func (p *ClickhouseProvider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		NewS3DescribeDataSource,
	}
}

func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &ClickhouseProvider{
			version: version,
		}
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
