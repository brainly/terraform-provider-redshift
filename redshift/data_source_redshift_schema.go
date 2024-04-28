package redshift

import (
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func dataSourceRedshiftSchema() *schema.Resource {
	return &schema.Resource{
		Description: `
A database contains one or more named schemas. Each schema in a database contains tables and other kinds of named objects. By default, a database has a single schema, which is named PUBLIC. You can use schemas to group database objects under a common name. Schemas are similar to file system directories, except that schemas cannot be nested.
`,
		ReadContext: RedshiftResourceFunc(dataSourceRedshiftSchemaRead),
		Schema: map[string]*schema.Schema{
			schemaNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of the schema.",
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			schemaOwnerAttr: {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Name of the schema owner.",
			},
			schemaQuotaAttr: {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "The maximum amount of disk space that the specified schema can use. GB is the default unit of measurement.",
			},
			schemaExternalSchemaAttr: {
				Type:        schema.TypeList,
				Optional:    true,
				Computed:    true,
				Description: "Configures the schema as an external schema. See https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_SCHEMA.html",
				MaxItems:    1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"database_name": {
							Type:        schema.TypeString,
							Computed:    true,
							Description: "The database where the external schema can be found",
						},
						"data_catalog_source": {
							Type:        schema.TypeList,
							Description: "Configures the external schema from the AWS Glue Data Catalog",
							Optional:    true,
							MaxItems:    1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"region": {
										Type:        schema.TypeString,
										Optional:    true,
										Computed:    true,
										Description: "If the external database is defined in an Athena data catalog or the AWS Glue Data Catalog, the AWS Region in which the database is located. This parameter is required if the database is defined in an external Data Catalog.",
									},
									"iam_role_arns": {
										Type:     schema.TypeList,
										Computed: true,
										Description: `The Amazon Resource Name (ARN) for the IAM roles that your cluster uses for authentication and authorization.
	As a minimum, the IAM roles must have permission to perform a LIST operation on the Amazon S3 bucket to be accessed and a GET operation on the Amazon S3 objects the bucket contains.
	If the external database is defined in an Amazon Athena data catalog or the AWS Glue Data Catalog, the IAM role must have permission to access Athena unless catalog_role is specified.
	For more information, see https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-iam-policies.html.

	When you attach a role to your cluster, your cluster can assume that role to access Amazon S3, Athena, and AWS Glue on your behalf.
	If a role attached to your cluster doesn't have access to the necessary resources, you can chain another role, possibly belonging to another account.
	Your cluster then temporarily assumes the chained role to access the data. You can also grant cross-account access by chaining roles.
	You can chain a maximum of 10 roles. Each role in the chain assumes the next role in the chain, until the cluster assumes the role at the end of chain.

	To chain roles, you establish a trust relationship between the roles. A role that assumes another role must have a permissions policy that allows it to assume the specified role.
	In turn, the role that passes permissions must have a trust policy that allows it to pass its permissions to another role.
	For more information, see https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html#authorizing-redshift-service-chaining-roles`,
										Elem: &schema.Schema{
											Type: schema.TypeString,
										},
									},
									"catalog_role_arns": {
										Type:     schema.TypeList,
										Optional: true,
										Computed: true,
										Description: `The Amazon Resource Name (ARN) for the IAM roles that your cluster uses for authentication and authorization for the data catalog.
	If this is not specified, Amazon Redshift uses the specified iam_role_arns. The catalog role must have permission to access the Data Catalog in AWS Glue or Athena.
	For more information, see https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-iam-policies.html.

	To chain roles, you establish a trust relationship between the roles. A role that assumes another role must have a permissions policy that allows it to assume the specified role.
	In turn, the role that passes permissions must have a trust policy that allows it to pass its permissions to another role.
	For more information, see https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html#authorizing-redshift-service-chaining-roles`,
										Elem: &schema.Schema{
											Type: schema.TypeString,
										},
									},
								},
							},
						},
						"hive_metastore_source": {
							Type:        schema.TypeList,
							Description: "Configures the external schema from a Hive Metastore.",
							Optional:    true,
							MaxItems:    1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"hostname": {
										Type:        schema.TypeString,
										Description: "The hostname of the hive metastore database.",
										Computed:    true,
									},
									"port": {
										Type:        schema.TypeInt,
										Description: "The port number of the hive metastore. The default port number is 9083.",
										Optional:    true,
										Computed:    true,
									},
									"iam_role_arns": {
										Type:     schema.TypeList,
										Computed: true,
										Description: `The Amazon Resource Name (ARN) for the IAM roles that your cluster uses for authentication and authorization.
	As a minimum, the IAM roles must have permission to perform a LIST operation on the Amazon S3 bucket to be accessed and a GET operation on the Amazon S3 objects the bucket contains.
	If the external database is defined in an Amazon Athena data catalog or the AWS Glue Data Catalog, the IAM role must have permission to access Athena unless catalog_role is specified.
	For more information, see https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-iam-policies.html.

	When you attach a role to your cluster, your cluster can assume that role to access Amazon S3, Athena, and AWS Glue on your behalf.
	If a role attached to your cluster doesn't have access to the necessary resources, you can chain another role, possibly belonging to another account.
	Your cluster then temporarily assumes the chained role to access the data. You can also grant cross-account access by chaining roles.
	You can chain a maximum of 10 roles. Each role in the chain assumes the next role in the chain, until the cluster assumes the role at the end of chain.

	To chain roles, you establish a trust relationship between the roles. A role that assumes another role must have a permissions policy that allows it to assume the specified role.
	In turn, the role that passes permissions must have a trust policy that allows it to pass its permissions to another role.
	For more information, see https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html#authorizing-redshift-service-chaining-roles`,
										Elem: &schema.Schema{
											Type: schema.TypeString,
										},
									},
								},
							},
						},
						"rds_postgres_source": {
							Type:        schema.TypeList,
							Description: "Configures the external schema to reference data using a federated query to RDS POSTGRES or Aurora PostgreSQL.",
							Optional:    true,
							MaxItems:    1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"hostname": {
										Type:        schema.TypeString,
										Description: "The hostname of the head node of the PostgreSQL database replica set.",
										Computed:    true,
									},
									"port": {
										Type:        schema.TypeInt,
										Description: "The port number of the PostgreSQL database. The default port number is 5432.",
										Optional:    true,
										Computed:    true,
									},
									"schema": {
										Type:        schema.TypeString,
										Description: "The name of the PostgreSQL schema. The default schema is 'public'",
										Optional:    true,
										Computed:    true,
									},
									"iam_role_arns": {
										Type:     schema.TypeList,
										Computed: true,
										Description: `The Amazon Resource Name (ARN) for the IAM roles that your cluster uses for authentication and authorization.
	As a minimum, the IAM roles must have permission to perform a LIST operation on the Amazon S3 bucket to be accessed and a GET operation on the Amazon S3 objects the bucket contains.
	If the external database is defined in an Amazon Athena data catalog or the AWS Glue Data Catalog, the IAM role must have permission to access Athena unless catalog_role is specified.
	For more information, see https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-iam-policies.html.

	When you attach a role to your cluster, your cluster can assume that role to access Amazon S3, Athena, and AWS Glue on your behalf.
	If a role attached to your cluster doesn't have access to the necessary resources, you can chain another role, possibly belonging to another account.
	Your cluster then temporarily assumes the chained role to access the data. You can also grant cross-account access by chaining roles.
	You can chain a maximum of 10 roles. Each role in the chain assumes the next role in the chain, until the cluster assumes the role at the end of chain.

	To chain roles, you establish a trust relationship between the roles. A role that assumes another role must have a permissions policy that allows it to assume the specified role.
	In turn, the role that passes permissions must have a trust policy that allows it to pass its permissions to another role.
	For more information, see https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html#authorizing-redshift-service-chaining-roles`,
										Elem: &schema.Schema{
											Type: schema.TypeString,
										},
									},
									"secret_arn": {
										Type: schema.TypeString,
										Description: `The Amazon Resource Name (ARN) of a supported PostgreSQL database engine secret created using AWS Secrets Manager.
	For information about how to create and retrieve an ARN for a secret, see https://docs.aws.amazon.com/secretsmanager/latest/userguide/manage_create-basic-secret.html
	and https://docs.aws.amazon.com/secretsmanager/latest/userguide/manage_retrieve-secret.html in the AWS Secrets Manager User Guide.`,
										Computed: true,
									},
								},
							},
						},
						"rds_mysql_source": {
							Type:        schema.TypeList,
							Description: "Configures the external schema to reference data using a federated query to RDS MYSQL or Aurora MySQL.",
							Optional:    true,
							MaxItems:    1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"hostname": {
										Type:        schema.TypeString,
										Description: "The hostname of the head node of the MySQL database replica set.",
										Computed:    true,
									},
									"port": {
										Type:        schema.TypeInt,
										Description: "The port number of the MySQL database. The default port number is 3306.",
										Optional:    true,
										Computed:    true,
									},
									"iam_role_arns": {
										Type:     schema.TypeList,
										Computed: true,
										Description: `The Amazon Resource Name (ARN) for the IAM roles that your cluster uses for authentication and authorization.
	As a minimum, the IAM roles must have permission to perform a LIST operation on the Amazon S3 bucket to be accessed and a GET operation on the Amazon S3 objects the bucket contains.
	If the external database is defined in an Amazon Athena data catalog or the AWS Glue Data Catalog, the IAM role must have permission to access Athena unless catalog_role is specified.
	For more information, see https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-iam-policies.html.

	When you attach a role to your cluster, your cluster can assume that role to access Amazon S3, Athena, and AWS Glue on your behalf.
	If a role attached to your cluster doesn't have access to the necessary resources, you can chain another role, possibly belonging to another account.
	Your cluster then temporarily assumes the chained role to access the data. You can also grant cross-account access by chaining roles.
	You can chain a maximum of 10 roles. Each role in the chain assumes the next role in the chain, until the cluster assumes the role at the end of chain.

	To chain roles, you establish a trust relationship between the roles. A role that assumes another role must have a permissions policy that allows it to assume the specified role.
	In turn, the role that passes permissions must have a trust policy that allows it to pass its permissions to another role.
	For more information, see https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html#authorizing-redshift-service-chaining-roles`,
										Elem: &schema.Schema{
											Type: schema.TypeString,
										},
									},
									"secret_arn": {
										Type: schema.TypeString,
										Description: `The Amazon Resource Name (ARN) of a supported MySQL database engine secret created using AWS Secrets Manager.
	For information about how to create and retrieve an ARN for a secret, see https://docs.aws.amazon.com/secretsmanager/latest/userguide/manage_create-basic-secret.html
	and https://docs.aws.amazon.com/secretsmanager/latest/userguide/manage_retrieve-secret.html in the AWS Secrets Manager User Guide.`,
										Computed: true,
									},
								},
							},
						},
						"redshift_source": {
							Type:        schema.TypeList,
							Description: "Configures the external schema to reference datashare database.",
							Optional:    true,
							MaxItems:    1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"schema": {
										Type:        schema.TypeString,
										Description: "The name of the datashare schema. The default schema is 'public'.",
										Optional:    true,
										Computed:    true,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func dataSourceRedshiftSchemaRead(db *DBConnection, d *schema.ResourceData) error {
	var schemaOwner, schemaId, schemaType string

	// Step 1: get basic schema info
	err := db.QueryRow(`
			SELECT
				pg_namespace.oid,
				TRIM(pg_user_info.usename),
				TRIM(svv_all_schemas.schema_type)
			FROM svv_all_schemas
			INNER JOIN pg_namespace ON (svv_all_schemas.database_name = $1 AND svv_all_schemas.schema_name = pg_namespace.nspname)
	LEFT JOIN pg_user_info
		ON (svv_all_schemas.database_name = $1 AND pg_user_info.usesysid = svv_all_schemas.schema_owner)
	WHERE svv_all_schemas.database_name = $1
	AND svv_all_schemas.schema_name = $2`, db.client.databaseName, d.Get(schemaNameAttr).(string)).Scan(&schemaId, &schemaOwner, &schemaType)
	if err != nil {
		return err
	}
	d.SetId(schemaId)
	d.Set(schemaOwnerAttr, schemaOwner)

	switch {
	case schemaType == "local":
		return resourceRedshiftSchemaReadLocal(db, d)
	case schemaType == "external":
		return resourceRedshiftSchemaReadExternal(db, d)
	default:
		return fmt.Errorf(`Unsupported schema type "%s". Supported types are "local" and "external".`, schemaType)
	}
}
