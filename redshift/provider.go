package redshift

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

const (
	defaultProviderMaxOpenConnections = 20
)

func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"host": {
				Type:        schema.TypeString,
				Description: "Name of Redshift server address to connect to.",
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("REDSHIFT_HOST", ""),
			},
			"username": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("REDSHIFT_USER", "root"),
				Description: "Redshift user name to connect as.",
			},
			"password": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("REDSHIFT_PASSWORD", nil),
				Description: "Password to be used if the Redshift server demands password authentication.",
				Sensitive:   true,
			},
			"port": {
				Type:        schema.TypeInt,
				Description: "The Redshift port number to connect to at the server host.",
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("REDSHIFT_PORT", 5439),
			},
			"sslmode": {
				Type:        schema.TypeString,
				Description: "This option determines whether or with what priority a secure SSL TCP/IP connection will be negotiated with the Redshift server. Valid values are `require` (default, always SSL, also skip verification), `verify-ca` (always SSL, verify that the certificate presented by the server was signed by a trusted CA), `verify-full` (always SSL, verify that the certification presented by the server was signed by a trusted CA and the server host name matches the one in the certificate), `disable` (no SSL).",
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("REDSHIFT_SSLMODE", "require"),
				ValidateFunc: validation.StringInSlice([]string{
					"require",
					"disable",
					"verify-ca",
					"verify-full",
				}, false),
			},
			"database": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "The name of the database to connect to. The default is `redshift`.",
				DefaultFunc: schema.EnvDefaultFunc("REDSHIFT_DATABASE", "redshift"),
			},
			"max_connections": {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      defaultProviderMaxOpenConnections,
				Description:  "Maximum number of connections to establish to the database. Zero means unlimited.",
				ValidateFunc: validation.IntAtLeast(-1),
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"redshift_user":      redshiftUser(),
			"redshift_group":     redshiftGroup(),
			"redshift_schema":    redshiftSchema(),
			"redshift_privilege": redshiftPrivilege(),
		},
		DataSourcesMap: map[string]*schema.Resource{},
		ConfigureFunc:  providerConfigure,
	}
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	config := Config{
		Host:     d.Get("host").(string),
		Port:     d.Get("port").(int),
		Username: d.Get("username").(string),
		Password: d.Get("password").(string),
		Database: d.Get("database").(string),
		SSLMode:  d.Get("sslmode").(string),
		MaxConns: d.Get("max_connections").(int),
	}

	client := config.NewClient(d.Get("database").(string))
	return client, nil
}
