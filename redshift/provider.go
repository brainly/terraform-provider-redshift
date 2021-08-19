package redshift

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
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
				ConflictsWith: []string{
					"temporary_credentials",
				},
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
			"temporary_credentials": {
				Type:        schema.TypeList,
				Optional:    true,
				Description: "Configuration for obtaining a temporary password using redshift:GetClusterCredentials",
				MaxItems:    1,
				ConflictsWith: []string{
					"password",
				},
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"cluster_identifier": {
							Type:         schema.TypeString,
							Required:     true,
							Description:  "The unique identifier of the cluster that contains the database for which you are requesting credentials. This parameter is case sensitive.",
							ValidateFunc: validation.StringLenBetween(1, 2147483647),
						},
						"auto_create_user": {
							Type:        schema.TypeBool,
							Optional:    true,
							Description: "Create a database user with the name specified for the user if one does not exist.",
							Default:     false,
						},
						"db_groups": {
							Type:        schema.TypeSet,
							Set:         schema.HashString,
							Optional:    true,
							Description: "A list of the names of existing database groups that the user will join for the current session, in addition to any group memberships for an existing user. If not specified, a new user is added only to PUBLIC.",
							MaxItems:    2147483647,
							Elem: &schema.Schema{
								Type:         schema.TypeString,
								ValidateFunc: dbGroupValidate,
							},
						},
						"duration_seconds": {
							Type:         schema.TypeInt,
							Optional:     true,
							Description:  "The number of seconds until the returned temporary password expires.",
							ValidateFunc: validation.IntBetween(900, 3600),
						},
					},
				},
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"redshift_user":      redshiftUser(),
			"redshift_group":     redshiftGroup(),
			"redshift_schema":    redshiftSchema(),
			"redshift_privilege": redshiftPrivilege(),
			"redshift_database":  redshiftDatabase(),
			"redshift_datashare": redshiftDatashare(),
		},
		DataSourcesMap: map[string]*schema.Resource{
			"redshift_user":      dataSourceRedshiftUser(),
			"redshift_group":     dataSourceRedshiftGroup(),
			"redshift_schema":    dataSourceRedshiftSchema(),
			"redshift_database":  dataSourceRedshiftDatabase(),
			"redshift_namespace": dataSourceRedshiftNamespace(),
		},
		ConfigureFunc: providerConfigure,
	}
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	username, password, err := resolveCredentials(d)
	if err != nil {
		return nil, err
	}
	config := Config{
		Host:     d.Get("host").(string),
		Port:     d.Get("port").(int),
		Username: username,
		Password: password,
		Database: d.Get("database").(string),
		SSLMode:  d.Get("sslmode").(string),
		MaxConns: d.Get("max_connections").(int),
	}

	log.Println("[DEBUG] creating database client")
	client := config.NewClient(d.Get("database").(string))
	log.Println("[DEBUG] created database client")
	return client, nil
}

func resolveCredentials(d *schema.ResourceData) (string, string, error) {
	username, ok := d.GetOk("username")
	if (!ok) || username == nil {
		return "", "", fmt.Errorf("Username is required")
	}
	password, passwordIsSet := d.GetOk("password")
	_, clusterIdentifierIsSet := d.GetOk("temporary_credentials.0.cluster_identifier")
	if !passwordIsSet && !clusterIdentifierIsSet {
		return "", "", fmt.Errorf("password or temporary_credentials must be configured")
	}
	if passwordIsSet {
		if password.(string) != "" {
			log.Println("[DEBUG] using password authentication")
			return username.(string), password.(string), nil
		}
	}
	log.Println("[DEBUG] using temporary credentials authentication")
	dbUser, dbPassword, err := temporaryCredentials(username.(string), d)
	log.Printf("[DEBUG] got temporary credentials with username %s\n", dbUser)
	return dbUser, dbPassword, err
}

// temporaryCredentials gets temporary credentials using GetClusterCredentials
func temporaryCredentials(username string, d *schema.ResourceData) (string, string, error) {
	sdkClient, err := redshiftSdkClient(d)
	if err != nil {
		return "", "", err
	}
	clusterIdentifier, clusterIdentifierIsSet := d.GetOk("temporary_credentials.0.cluster_identifier")
	if !clusterIdentifierIsSet {
		return "", "", fmt.Errorf("temporary_credentials not configured")
	}
	input := &redshift.GetClusterCredentialsInput{
		ClusterIdentifier: aws.String(clusterIdentifier.(string)),
		DbName:            aws.String(d.Get("database").(string)),
		DbUser:            aws.String(username),
	}
	if autoCreateUser, ok := d.GetOk("temporary_credentials.0.auto_create_user"); ok {
		input.AutoCreate = aws.Bool(autoCreateUser.(bool))
	}
	if dbGroups, ok := d.GetOk("temporary_credentials.0.db_groups"); ok {
		if dbGroups != nil {
			dbGroupsList := dbGroups.(*schema.Set).List()
			if len(dbGroupsList) > 0 {
				var groups []string
				for _, group := range dbGroupsList {
					if group.(string) != "" {
						groups = append(groups, group.(string))
					}
				}
				input.DbGroups = groups
			}
		}
	}
	if durationSeconds, ok := d.GetOk("temporary_credentials.0.duration_seconds"); ok {
		duration := durationSeconds.(int)
		if duration > 0 {
			input.DurationSeconds = aws.Int32(int32(duration))
		}
	}
	log.Println("[DEBUG] making GetClusterCredentials request")
	response, err := sdkClient.GetClusterCredentials(context.TODO(), input)
	if err != nil {
		return "", "", err
	}
	return aws.ToString(response.DbUser), aws.ToString(response.DbPassword), nil
}

func redshiftSdkClient(d *schema.ResourceData) (*redshift.Client, error) {
	config, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	return redshift.NewFromConfig(config), nil
}
