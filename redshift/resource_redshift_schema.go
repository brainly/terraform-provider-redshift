package redshift

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/customdiff"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	schemaNameAttr            = "name"
	schemaOwnerAttr           = "owner"
	schemaQuotaAttr           = "quota"
	schemaCascadeOnDeleteAttr = "cascade_on_delete"
	schemaExternalSchemaAttr  = "external_schema"
	dataCatalogAttr           = "external_schema.0.data_catalog_source.0"
	hiveMetastoreAttr         = "external_schema.0.hive_metastore_source.0"
	rdsPostgresAttr           = "external_schema.0.rds_postgres_source.0"
	rdsMysqlAttr              = "external_schema.0.rds_mysql_source.0"
	redshiftAttr              = "external_schema.0.redshift_source.0"
)

func redshiftSchema() *schema.Resource {
	return &schema.Resource{
		Description: `
A database contains one or more named schemas. Each schema in a database contains tables and other kinds of named objects. By default, a database has a single schema, which is named PUBLIC. You can use schemas to group database objects under a common name. Schemas are similar to file system directories, except that schemas cannot be nested.
`,
		CreateContext: RedshiftResourceFunc(resourceRedshiftSchemaCreate),
		ReadContext:   RedshiftResourceFunc(resourceRedshiftSchemaRead),
		UpdateContext: RedshiftResourceFunc(resourceRedshiftSchemaUpdate),
		DeleteContext: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftSchemaDelete),
		),
		Exists: RedshiftResourceExistsFunc(resourceRedshiftSchemaExists),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		CustomizeDiff: forceNewIfListSizeChanged(schemaExternalSchemaAttr),
		Schema: map[string]*schema.Schema{
			schemaNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of the schema. The schema name can't be `PUBLIC`.",
				ValidateFunc: validation.StringNotInSlice([]string{
					"public",
				}, true),
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			schemaOwnerAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				Description: "Name of the schema owner.",
				StateFunc: func(val interface{}) string {
					return val.(string)
				},
			},
			schemaQuotaAttr: {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      0,
				Description:  "The maximum amount of disk space that the specified schema can use. GB is the default unit of measurement.",
				ValidateFunc: validation.IntAtLeast(0),
				StateFunc: func(val interface{}) string {
					return fmt.Sprintf("%d", val.(int)*1024)
				},
				ConflictsWith: []string{
					schemaExternalSchemaAttr,
				},
			},
			schemaCascadeOnDeleteAttr: {
				Type:        schema.TypeBool,
				Optional:    true,
				Description: "Indicates to automatically drop all objects in the schema. The default action is TO NOT drop a schema if it contains any objects.",
				ConflictsWith: []string{
					schemaExternalSchemaAttr,
				},
			},
			schemaExternalSchemaAttr: {
				Type:        schema.TypeList,
				Optional:    true,
				Description: "Configures the schema as an external schema. See https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_SCHEMA.html",
				MaxItems:    1,
				ConflictsWith: []string{
					schemaQuotaAttr,
					schemaCascadeOnDeleteAttr,
				},
				Elem: &schema.Resource{
					CustomizeDiff: customdiff.All(
						forceNewIfListSizeChanged("data_catalog_source"),
						forceNewIfListSizeChanged("hive_metastore_source"),
						forceNewIfListSizeChanged("rds_postgres_source"),
						forceNewIfListSizeChanged("rds_mysql_source"),
						forceNewIfListSizeChanged("redshift_source"),
					),
					Schema: map[string]*schema.Schema{
						"database_name": {
							Type:        schema.TypeString,
							Required:    true,
							Description: "The database where the external schema can be found",
							ForceNew:    true,
						},
						"data_catalog_source": {
							Type:        schema.TypeList,
							Description: "Configures the external schema from the AWS Glue Data Catalog",
							Optional:    true,
							MaxItems:    1,
							ConflictsWith: []string{
								fmt.Sprintf("%s.0.hive_metastore_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.rds_postgres_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.rds_mysql_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.redshift_source", schemaExternalSchemaAttr),
							},
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"region": {
										Type:        schema.TypeString,
										Optional:    true,
										Description: "If the external database is defined in an Athena data catalog or the AWS Glue Data Catalog, the AWS Region in which the database is located. This parameter is required if the database is defined in an external Data Catalog.",
										ForceNew:    true,
									},
									"iam_role_arns": {
										Type:     schema.TypeList,
										Required: true,
										MinItems: 1,
										MaxItems: 10,
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
										ForceNew: true,
										Elem: &schema.Schema{
											Type: schema.TypeString,
										},
									},
									"catalog_role_arns": {
										Type:     schema.TypeList,
										Optional: true,
										MinItems: 1,
										MaxItems: 10,
										Description: `The Amazon Resource Name (ARN) for the IAM roles that your cluster uses for authentication and authorization for the data catalog.
	If this is not specified, Amazon Redshift uses the specified iam_role_arns. The catalog role must have permission to access the Data Catalog in AWS Glue or Athena.
	For more information, see https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-iam-policies.html.

  To chain roles, you establish a trust relationship between the roles. A role that assumes another role must have a permissions policy that allows it to assume the specified role.
	In turn, the role that passes permissions must have a trust policy that allows it to pass its permissions to another role.
	For more information, see https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html#authorizing-redshift-service-chaining-roles`,
										ForceNew: true,
										Elem: &schema.Schema{
											Type: schema.TypeString,
										},
									},
									"create_external_database_if_not_exists": {
										Type:     schema.TypeBool,
										Optional: true,
										Default:  false,
										DiffSuppressFunc: func(k, old, new string, d *schema.ResourceData) bool {
											// If the old value is empty, and the new value is not, it means we are creating the resource.
											// This must trigger diff in order to save proper value in state.
											if old == "" && new != "" {
												return false
											}
											return true
										},
										Description: `When enabled, creates an external database with the name specified by the database argument,
	if the specified external database doesn't exist. If the specified external database exists, the command makes no changes.
	In this case, the command returns a message that the external database exists, rather than terminating with an error.

  To use create_external_database_if_not_exists with a Data Catalog enabled for AWS Lake Formation, you need CREATE_DATABASE permission on the Data Catalog.`,
									},
								},
							},
						},
						"hive_metastore_source": {
							Type:        schema.TypeList,
							Description: "Configures the external schema from a Hive Metastore.",
							Optional:    true,
							MaxItems:    1,
							ConflictsWith: []string{
								fmt.Sprintf("%s.0.data_catalog_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.rds_postgres_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.rds_mysql_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.redshift_source", schemaExternalSchemaAttr),
							},
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"hostname": {
										Type:        schema.TypeString,
										Description: "The hostname of the hive metastore database.",
										Required:    true,
										ForceNew:    true,
									},
									"port": {
										Type:         schema.TypeInt,
										Description:  "The port number of the hive metastore. The default port number is 9083.",
										Optional:     true,
										Default:      9083,
										ValidateFunc: validation.IntBetween(1, 65535),
										ForceNew:     true,
									},
									"iam_role_arns": {
										Type:     schema.TypeList,
										Required: true,
										MinItems: 1,
										MaxItems: 10,
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
										ForceNew: true,
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
							ConflictsWith: []string{
								fmt.Sprintf("%s.0.data_catalog_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.hive_metastore_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.rds_mysql_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.redshift_source", schemaExternalSchemaAttr),
							},
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"hostname": {
										Type:        schema.TypeString,
										Description: "The hostname of the head node of the PostgreSQL database replica set.",
										Required:    true,
										ForceNew:    true,
									},
									"port": {
										Type:         schema.TypeInt,
										Description:  "The port number of the PostgreSQL database. The default port number is 5432.",
										Optional:     true,
										Default:      5432,
										ValidateFunc: validation.IntBetween(1, 65535),
										ForceNew:     true,
									},
									"schema": {
										Type:        schema.TypeString,
										Description: "The name of the PostgreSQL schema. The default schema is 'public'",
										Optional:    true,
										Default:     "public",
										ForceNew:    true,
									},
									"iam_role_arns": {
										Type:     schema.TypeList,
										Required: true,
										MinItems: 1,
										MaxItems: 10,
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
										ForceNew: true,
										Elem: &schema.Schema{
											Type: schema.TypeString,
										},
									},
									"secret_arn": {
										Type: schema.TypeString,
										Description: `The Amazon Resource Name (ARN) of a supported PostgreSQL database engine secret created using AWS Secrets Manager.
	For information about how to create and retrieve an ARN for a secret, see https://docs.aws.amazon.com/secretsmanager/latest/userguide/manage_create-basic-secret.html
	and https://docs.aws.amazon.com/secretsmanager/latest/userguide/manage_retrieve-secret.html in the AWS Secrets Manager User Guide.`,
										Required: true,
										ForceNew: true,
									},
								},
							},
						},
						"rds_mysql_source": {
							Type:        schema.TypeList,
							Description: "Configures the external schema to reference data using a federated query to RDS MYSQL or Aurora MySQL.",
							Optional:    true,
							MaxItems:    1,
							ConflictsWith: []string{
								fmt.Sprintf("%s.0.data_catalog_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.hive_metastore_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.rds_postgres_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.redshift_source", schemaExternalSchemaAttr),
							},
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"hostname": {
										Type:        schema.TypeString,
										Description: "The hostname of the head node of the MySQL database replica set.",
										Required:    true,
										ForceNew:    true,
									},
									"port": {
										Type:         schema.TypeInt,
										Description:  "The port number of the MySQL database. The default port number is 3306.",
										Optional:     true,
										Default:      3306,
										ValidateFunc: validation.IntBetween(1, 65535),
										ForceNew:     true,
									},
									"iam_role_arns": {
										Type:     schema.TypeList,
										Required: true,
										MinItems: 1,
										MaxItems: 10,
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
										ForceNew: true,
										Elem: &schema.Schema{
											Type: schema.TypeString,
										},
									},
									"secret_arn": {
										Type: schema.TypeString,
										Description: `The Amazon Resource Name (ARN) of a supported MySQL database engine secret created using AWS Secrets Manager.
	For information about how to create and retrieve an ARN for a secret, see https://docs.aws.amazon.com/secretsmanager/latest/userguide/manage_create-basic-secret.html
	and https://docs.aws.amazon.com/secretsmanager/latest/userguide/manage_retrieve-secret.html in the AWS Secrets Manager User Guide.`,
										Required: true,
										ForceNew: true,
									},
								},
							},
						},
						"redshift_source": {
							Type:        schema.TypeList,
							Description: "Configures the external schema to reference datashare database.",
							Optional:    true,
							MaxItems:    1,
							ConflictsWith: []string{
								fmt.Sprintf("%s.0.data_catalog_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.hive_metastore_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.rds_postgres_source", schemaExternalSchemaAttr),
								fmt.Sprintf("%s.0.rds_mysql_source", schemaExternalSchemaAttr),
							},
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"schema": {
										Type:        schema.TypeString,
										Description: "The name of the datashare schema. The default schema is 'public'.",
										Optional:    true,
										Default:     "public",
										ForceNew:    true,
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

func resourceRedshiftSchemaExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	var name string
	err := db.QueryRow("SELECT nspname FROM pg_namespace WHERE oid = $1", d.Id()).Scan(&name)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func resourceRedshiftSchemaRead(db *DBConnection, d *schema.ResourceData) error {
	return resourceRedshiftSchemaReadImpl(db, d)
}

func resourceRedshiftSchemaReadImpl(db *DBConnection, d *schema.ResourceData) error {
	var schemaOwner, schemaName, schemaType string

	// Step 1: get basic schema info
	err := db.QueryRow(`
			SELECT
				TRIM(svv_all_schemas.schema_name),
				TRIM(pg_user_info.usename),
				TRIM(svv_all_schemas.schema_type)
			FROM svv_all_schemas
			INNER JOIN pg_namespace ON (svv_all_schemas.database_name = $1 AND svv_all_schemas.schema_name = pg_namespace.nspname)
	LEFT JOIN pg_user_info
		ON (svv_all_schemas.database_name = $1 AND pg_user_info.usesysid = svv_all_schemas.schema_owner)
	WHERE svv_all_schemas.database_name = $1
	AND pg_namespace.oid = $2`, db.client.databaseName, d.Id()).Scan(&schemaName, &schemaOwner, &schemaType)
	if err != nil {
		return err
	}
	d.Set(schemaNameAttr, schemaName)
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

func resourceRedshiftSchemaReadLocal(db *DBConnection, d *schema.ResourceData) error {
	var schemaQuota = 0
	isServerless, err := db.client.config.IsServerless(db)
	if err != nil {
		return err
	}

	if isServerless {
		err = db.QueryRow(`
			SELECT COALESCE(quota, 0)
			FROM svv_redshift_schema_quota
			WHERE database_name = $1 
			  AND schema_name = $2
		`, db.client.databaseName, d.Get(schemaNameAttr)).Scan(&schemaQuota)
	} else {
		err = db.QueryRow(`
			SELECT
			COALESCE(quota, 0)
			FROM svv_schema_quota_state
			WHERE schema_id = $1
		`, d.Id()).Scan(&schemaQuota)
	}
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	d.Set(schemaQuotaAttr, schemaQuota)
	d.Set(schemaExternalSchemaAttr, nil)

	return nil
}

func resourceRedshiftSchemaReadExternal(db *DBConnection, d *schema.ResourceData) error {
	var sourceType, sourceDbName, iamRole, catalogRole, region, sourceSchema, hostName, port, secretArn string
	err := db.QueryRow(`
	SELECT
		CASE
			WHEN eskind = 1 THEN 'data_catalog_source'
			WHEN eskind = 2 THEN 'hive_metastore_source'
			WHEN eskind = 3 THEN 'rds_postgres_source'
			WHEN eskind = 4 THEN 'redshift_source'
			WHEN eskind = 7 THEN 'rds_mysql_source'
			ELSE 'unknown'
		END,
		TRIM(databasename),
		COALESCE(CASE WHEN is_valid_json(esoptions) THEN json_extract_path_text(esoptions, 'IAM_ROLE') END, ''),
		COALESCE(CASE WHEN is_valid_json(esoptions) THEN json_extract_path_text(esoptions, 'CATALOG_ROLE') END, ''),
		COALESCE(CASE WHEN is_valid_json(esoptions) THEN json_extract_path_text(esoptions, 'REGION') END, ''),
		COALESCE(CASE WHEN is_valid_json(esoptions) THEN json_extract_path_text(esoptions, 'SCHEMA') END, ''),
		COALESCE(CASE WHEN is_valid_json(esoptions) THEN json_extract_path_text(esoptions, 'URI') END, ''),
		COALESCE(CASE WHEN is_valid_json(esoptions) THEN json_extract_path_text(esoptions, 'PORT') END, ''),
		COALESCE(CASE WHEN is_valid_json(esoptions) THEN json_extract_path_text(esoptions, 'SECRET_ARN') END, '')
	FROM
	  svv_external_schemas
	WHERE
	  esoid = $1`, d.Id()).Scan(&sourceType, &sourceDbName, &iamRole, &catalogRole, &region, &sourceSchema, &hostName, &port, &secretArn)

	if err != nil {
		return err
	}
	externalSchemaConfiguration := make(map[string]interface{})
	sourceConfiguration := make(map[string]interface{})
	externalSchemaConfiguration["database_name"] = &sourceDbName
	switch {
	case sourceType == "data_catalog_source":
		sourceConfiguration["region"] = &region
		sourceConfiguration["iam_role_arns"], err = splitCsvAndTrim(iamRole)
		if err != nil {
			return fmt.Errorf("Error parsing iam_role_arns: %v", err)
		}
		sourceConfiguration["catalog_role_arns"], err = splitCsvAndTrim(catalogRole)
		if err != nil {
			return fmt.Errorf("Error parsing catalog_role_arns: %v", err)
		}
	case sourceType == "hive_metastore_source":
		sourceConfiguration["hostname"] = &hostName
		if port != "" {
			portNum, err := strconv.Atoi(port)
			if err != nil {
				return fmt.Errorf("hive_metastore_source port was not an integer")
			}
			sourceConfiguration["port"] = &portNum
		}
		sourceConfiguration["iam_role_arns"], err = splitCsvAndTrim(iamRole)
		if err != nil {
			return fmt.Errorf("Error parsing iam_role_arns: %v", err)
		}
	case sourceType == "rds_postgres_source":
		sourceConfiguration["hostname"] = &hostName
		if port != "" {
			portNum, err := strconv.Atoi(port)
			if err != nil {
				return fmt.Errorf("rds_postgres_source port was not an integer")
			}
			sourceConfiguration["port"] = &portNum
		}
		if sourceSchema != "" {
			sourceConfiguration["schema"] = &sourceSchema
		}
		sourceConfiguration["iam_role_arns"], err = splitCsvAndTrim(iamRole)
		if err != nil {
			return fmt.Errorf("Error parsing iam_role_arns: %v", err)
		}
		sourceConfiguration["secret_arn"] = &secretArn
	case sourceType == "rds_mysql_source":
		sourceConfiguration["hostname"] = &hostName
		if port != "" {
			portNum, err := strconv.Atoi(port)
			if err != nil {
				return fmt.Errorf("rds_mysql_source port was not an integer")
			}
			sourceConfiguration["port"] = &portNum
		}
		sourceConfiguration["iam_role_arns"], err = splitCsvAndTrim(iamRole)
		if err != nil {
			return fmt.Errorf("Error parsing iam_role_arns: %v", err)
		}
		sourceConfiguration["secret_arn"] = &secretArn
	case sourceType == "redshift_source":
		if sourceSchema != "" {
			sourceConfiguration["schema"] = &sourceSchema
		}
	default:
		return fmt.Errorf(`Unsupported source database type %s`, sourceType)
	}
	externalSchemaConfiguration[sourceType] = []map[string]interface{}{sourceConfiguration}

	d.Set(schemaQuotaAttr, 0)
	d.Set(schemaExternalSchemaAttr, []map[string]interface{}{externalSchemaConfiguration})

	return nil
}

func resourceRedshiftSchemaDelete(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)
	schemaName := d.Get(schemaNameAttr).(string)

	cascade_or_restrict := "RESTRICT"
	if cascade, ok := d.GetOk(schemaCascadeOnDeleteAttr); ok && cascade.(bool) {
		cascade_or_restrict = "CASCADE"
	}

	query := fmt.Sprintf("DROP SCHEMA %s %s", pq.QuoteIdentifier(schemaName), cascade_or_restrict)
	if _, err := tx.Exec(query); err != nil {
		return err
	}

	return tx.Commit()
}

func resourceRedshiftSchemaCreate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if _, isExternal := d.GetOk(fmt.Sprintf("%s.0.%s", schemaExternalSchemaAttr, "database_name")); isExternal {
		err = resourceRedshiftSchemaCreateExternal(tx, d)
	} else {
		err = resourceRedshiftSchemaCreateInternal(tx, d)
	}
	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftSchemaReadImpl(db, d)
}

func resourceRedshiftSchemaCreateInternal(tx *sql.Tx, d *schema.ResourceData) error {
	schemaName := d.Get(schemaNameAttr).(string)
	schemaQuota := d.Get(schemaQuotaAttr).(int)
	createOpts := []string{}

	if v, ok := d.GetOk(schemaOwnerAttr); ok {
		createOpts = append(createOpts, fmt.Sprintf("AUTHORIZATION %s", pq.QuoteIdentifier(v.(string))))
	}

	quotaValue := "QUOTA UNLIMITED"
	if schemaQuota > 0 {
		quotaValue = fmt.Sprintf("QUOTA %d GB", schemaQuota)
	}
	createOpts = append(createOpts, quotaValue)

	query := fmt.Sprintf("CREATE SCHEMA %s %s", pq.QuoteIdentifier(schemaName), strings.Join(createOpts, " "))

	if _, err := tx.Exec(query); err != nil {
		return err
	}

	var schemaOID string
	if err := tx.QueryRow("SELECT oid FROM pg_namespace WHERE nspname = $1", strings.ToLower(schemaName)).Scan(&schemaOID); err != nil {
		return err
	}

	d.SetId(schemaOID)

	return nil
}

func resourceRedshiftSchemaCreateExternal(tx *sql.Tx, d *schema.ResourceData) error {
	schemaName := d.Get(schemaNameAttr).(string)
	query := fmt.Sprintf("CREATE EXTERNAL SCHEMA %s", pq.QuoteIdentifier(schemaName))
	sourceDbName := d.Get(fmt.Sprintf("%s.0.%s", schemaExternalSchemaAttr, "database_name")).(string)
	var configQuery string
	if _, isDataCatalog := d.GetOk(dataCatalogAttr); isDataCatalog {
		// data catalog source
		configQuery = getDataCatalogConfigQueryPart(d, sourceDbName)
	} else if _, isHiveMetastore := d.GetOk(hiveMetastoreAttr); isHiveMetastore {
		// hive metastore source
		configQuery = getHiveMetastoreConfigQueryPart(d, sourceDbName)
	} else if _, isRdsPostgres := d.GetOk(rdsPostgresAttr); isRdsPostgres {
		// rds postgres source
		configQuery = getRdsPostgresConfigQueryPart(d, sourceDbName)
	} else if _, isRdsMysql := d.GetOk(rdsMysqlAttr); isRdsMysql {
		// rds mysql source
		configQuery = getRdsMysqlConfigQueryPart(d, sourceDbName)
	} else if _, isRedshift := d.GetOk(redshiftAttr); isRedshift {
		// redshift source
		configQuery = getRedshiftConfigQueryPart(d, sourceDbName)
	} else {
		return fmt.Errorf("Can't create external schema. No source configuration found.")
	}

	query = fmt.Sprintf("%s %s", query, configQuery)

	log.Printf("[DEBUG] creating external schema: %s\n", query)
	if _, err := tx.Exec(query); err != nil {
		return err
	}

	if v, ok := d.GetOk(schemaOwnerAttr); ok {
		query = fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(v.(string)))
		log.Printf("[DEBUG] setting schema owner: %s\n", query)
		if _, err := tx.Exec(query); err != nil {
			return err
		}
	}

	var schemaOID string
	if err := tx.QueryRow("SELECT oid FROM pg_namespace WHERE nspname = $1", strings.ToLower(schemaName)).Scan(&schemaOID); err != nil {
		return err
	}

	d.SetId(schemaOID)

	return nil
}

func getDataCatalogConfigQueryPart(d *schema.ResourceData, sourceDbName string) string {
	query := fmt.Sprintf("FROM DATA CATALOG DATABASE '%s'", pqQuoteLiteral(sourceDbName))
	if region, hasRegion := d.GetOk(fmt.Sprintf("%s.%s", dataCatalogAttr, "region")); hasRegion {
		query = fmt.Sprintf("%s REGION '%s'", query, pqQuoteLiteral(region.(string)))
	}
	iamRoleArnsRaw := d.Get(fmt.Sprintf("%s.%s", dataCatalogAttr, "iam_role_arns")).([]interface{})
	iamRoleArns := []string{}
	for _, arn := range iamRoleArnsRaw {
		iamRoleArns = append(iamRoleArns, arn.(string))
	}
	query = fmt.Sprintf("%s IAM_ROLE '%s'", query, pqQuoteLiteral(strings.Join(iamRoleArns, ",")))
	if catalogRoleArnsRaw, hasCatalogRoleArns := d.GetOk(fmt.Sprintf("%s.%s", dataCatalogAttr, "catalog_role_arns")); hasCatalogRoleArns {
		catalogRoleArns := []string{}
		for _, arn := range catalogRoleArnsRaw.([]interface{}) {
			catalogRoleArns = append(catalogRoleArns, arn.(string))
		}
		if len(catalogRoleArns) > 0 {
			query = fmt.Sprintf("%s CATALOG_ROLE '%s'", query, pqQuoteLiteral(strings.Join(catalogRoleArns, ",")))
		}
	}
	if d.Get(fmt.Sprintf("%s.%s", dataCatalogAttr, "create_external_database_if_not_exists")).(bool) {
		query = fmt.Sprintf("%s CREATE EXTERNAL DATABASE IF NOT EXISTS", query)
	}
	return query
}

func getHiveMetastoreConfigQueryPart(d *schema.ResourceData, sourceDbName string) string {
	query := fmt.Sprintf("FROM HIVE METASTORE DATABASE '%s'", pqQuoteLiteral(sourceDbName))
	hostName := d.Get(fmt.Sprintf("%s.%s", hiveMetastoreAttr, "hostname")).(string)
	query = fmt.Sprintf("%s URI '%s'", query, pqQuoteLiteral(hostName))
	if port, portIsSet := d.GetOk(fmt.Sprintf("%s.%s", hiveMetastoreAttr, "port")); portIsSet {
		query = fmt.Sprintf("%s PORT %d", query, port.(int))
	}
	iamRoleArnsRaw := d.Get(fmt.Sprintf("%s.%s", hiveMetastoreAttr, "iam_role_arns")).([]interface{})
	iamRoleArns := []string{}
	for _, arn := range iamRoleArnsRaw {
		iamRoleArns = append(iamRoleArns, arn.(string))
	}
	query = fmt.Sprintf("%s IAM_ROLE '%s'", query, pqQuoteLiteral(strings.Join(iamRoleArns, ",")))
	return query
}

func getRdsPostgresConfigQueryPart(d *schema.ResourceData, sourceDbName string) string {
	query := fmt.Sprintf("FROM POSTGRES DATABASE '%s'", pqQuoteLiteral(sourceDbName))
	if sourceSchema, sourceSchemaIsSet := d.GetOk(fmt.Sprintf("%s.%s", rdsPostgresAttr, "schema")); sourceSchemaIsSet {
		query = fmt.Sprintf("%s SCHEMA '%s'", query, pqQuoteLiteral(sourceSchema.(string)))
	}
	hostName := d.Get(fmt.Sprintf("%s.%s", rdsPostgresAttr, "hostname")).(string)
	query = fmt.Sprintf("%s URI '%s'", query, pqQuoteLiteral(hostName))
	if port, portIsSet := d.GetOk(fmt.Sprintf("%s.%s", rdsPostgresAttr, "port")); portIsSet {
		query = fmt.Sprintf("%s PORT %d", query, port.(int))
	}
	iamRoleArnsRaw := d.Get(fmt.Sprintf("%s.%s", rdsPostgresAttr, "iam_role_arns")).([]interface{})
	iamRoleArns := []string{}
	for _, arn := range iamRoleArnsRaw {
		iamRoleArns = append(iamRoleArns, arn.(string))
	}
	query = fmt.Sprintf("%s IAM_ROLE '%s'", query, pqQuoteLiteral(strings.Join(iamRoleArns, ",")))
	secretArn := d.Get(fmt.Sprintf("%s.%s", rdsPostgresAttr, "secret_arn")).(string)
	query = fmt.Sprintf("%s SECRET_ARN '%s'", query, pqQuoteLiteral(secretArn))
	return query
}

func getRdsMysqlConfigQueryPart(d *schema.ResourceData, sourceDbName string) string {
	query := fmt.Sprintf("FROM MYSQL DATABASE '%s'", pqQuoteLiteral(sourceDbName))
	hostName := d.Get(fmt.Sprintf("%s.%s", rdsMysqlAttr, "hostname")).(string)
	query = fmt.Sprintf("%s URI '%s'", query, pqQuoteLiteral(hostName))
	if port, portIsSet := d.GetOk(fmt.Sprintf("%s.%s", rdsMysqlAttr, "port")); portIsSet {
		query = fmt.Sprintf("%s PORT %d", query, port.(int))
	}
	iamRoleArnsRaw := d.Get(fmt.Sprintf("%s.%s", rdsMysqlAttr, "iam_role_arns")).([]interface{})
	iamRoleArns := []string{}
	for _, arn := range iamRoleArnsRaw {
		iamRoleArns = append(iamRoleArns, arn.(string))
	}
	query = fmt.Sprintf("%s IAM_ROLE '%s'", query, pqQuoteLiteral(strings.Join(iamRoleArns, ",")))
	secretArn := d.Get(fmt.Sprintf("%s.%s", rdsMysqlAttr, "secret_arn")).(string)
	query = fmt.Sprintf("%s SECRET_ARN '%s'", query, pqQuoteLiteral(secretArn))
	return query
}

func getRedshiftConfigQueryPart(d *schema.ResourceData, sourceDbName string) string {
	query := fmt.Sprintf("FROM REDSHIFT DATABASE '%s'", pqQuoteLiteral(sourceDbName))
	if sourceSchema, sourceSchemaIsSet := d.GetOk(fmt.Sprintf("%s.%s", redshiftAttr, "schema")); sourceSchemaIsSet {
		query = fmt.Sprintf("%s SCHEMA '%s'", query, pqQuoteLiteral(sourceSchema.(string)))
	}
	return query
}

func resourceRedshiftSchemaUpdate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := setSchemaName(tx, d); err != nil {
		return err
	}

	if err := setSchemaOwner(tx, db, d); err != nil {
		return err
	}

	if err := setSchemaQuota(tx, d); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftSchemaReadImpl(db, d)
}

func setSchemaName(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(schemaNameAttr) {
		return nil
	}

	oldRaw, newRaw := d.GetChange(schemaNameAttr)
	oldValue := oldRaw.(string)
	newValue := newRaw.(string)

	if newValue == "" {
		return fmt.Errorf("Error setting schema name to an empty string")
	}

	query := fmt.Sprintf("ALTER SCHEMA %s RENAME TO %s", pq.QuoteIdentifier(oldValue), pq.QuoteIdentifier(newValue))
	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("Error updating schema NAME: %w", err)
	}

	return nil
}

func setSchemaOwner(tx *sql.Tx, db *DBConnection, d *schema.ResourceData) error {
	if !d.HasChange(schemaOwnerAttr) {
		return nil
	}

	schemaName := d.Get(schemaNameAttr).(string)
	schemaOwner := d.Get(schemaOwnerAttr).(string)

	_, err := tx.Exec(fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(schemaOwner)))
	return err
}

func setSchemaQuota(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(schemaQuotaAttr) {
		return nil
	}

	schemaName := d.Get(schemaNameAttr).(string)
	schemaQuota := d.Get(schemaQuotaAttr).(int)

	quotaValue := "UNLIMITED"
	if schemaQuota > 0 {
		quotaValue = fmt.Sprintf("%d GB", schemaQuota)
	}

	_, err := tx.Exec(fmt.Sprintf("ALTER SCHEMA %s QUOTA %s", pq.QuoteIdentifier(schemaName), quotaValue))
	return err
}
