package redshift

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccDataSourceRedshiftSchema_basic(t *testing.T) {
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_data_basic"), "-", "_")
	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftSchemaDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccDataSourceRedshiftSchemaConfig_basic(schemaName),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.redshift_schema.schema", schemaNameAttr, schemaName),
					resource.TestCheckResourceAttrSet("data.redshift_schema.schema", schemaOwnerAttr),
					resource.TestCheckResourceAttrSet("data.redshift_schema.schema", schemaQuotaAttr),
				),
			},
		},
	})
}

func testAccDataSourceRedshiftSchemaConfig_basic(schemaName string) string {
	return fmt.Sprintf(`
resource "redshift_schema" "schema" {
	%[1]s = %[2]q
}

data "redshift_schema" "schema" {
	%[1]s = redshift_schema.schema.%[1]s
}
`, schemaNameAttr, schemaName)
}

// Acceptance test for external redshift schema using AWS Glue Data Catalog
// The following environment variables must be set, otherwise the test will be skipped:
//
//	REDSHIFT_EXTERNAL_SCHEMA_DATA_CATALOG_DATABASE - source database name
//	REDSHIFT_EXTERNAL_SCHEMA_RDS_DATA_CATALOG_IAM_ROLE_ARNS - comma-separated list of ARNs to use
func TestAccDataSourceRedshiftSchema_ExternalDataCatalog(t *testing.T) {
	dbName := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_DATA_CATALOG_DATABASE", t)
	iamRoleArnsRaw := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_DATA_CATALOG_IAM_ROLE_ARNS", t)
	iamRoleArns := strings.Split(iamRoleArnsRaw, ",")
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_external_data_schema_data_catalog"), "-", "_")
	configCreate := fmt.Sprintf(`
resource "redshift_schema" "spectrum" {
	%[1]s = %[2]q
	%[3]s {
		database_name = %[4]q
		data_catalog_source {
			iam_role_arns = %[5]s
		}
	}
}

data "redshift_schema" "spectrum" {
	%[1]s = redshift_schema.spectrum.%[1]s
}
`,
		schemaNameAttr, schemaName, schemaExternalSchemaAttr, dbName, tfArray(iamRoleArns))
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftSchemaDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftSchemaExists(schemaName),
					resource.TestCheckResourceAttr("data.redshift_schema.spectrum", "name", schemaName),
					resource.TestCheckResourceAttr("data.redshift_schema.spectrum", fmt.Sprintf("%s.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("data.redshift_schema.spectrum", fmt.Sprintf("%s.0.database_name", schemaExternalSchemaAttr), dbName),
					resource.TestCheckResourceAttr("data.redshift_schema.spectrum", fmt.Sprintf("%s.0.data_catalog_source.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("data.redshift_schema.spectrum", fmt.Sprintf("%s.0.data_catalog_source.0.iam_role_arns.#", schemaExternalSchemaAttr), fmt.Sprintf("%d", len(iamRoleArns))),
					resource.ComposeTestCheckFunc(func() []resource.TestCheckFunc {
						results := []resource.TestCheckFunc{}
						for i, arn := range iamRoleArns {
							results = append(results, resource.TestCheckResourceAttr("data.redshift_schema.spectrum", fmt.Sprintf("%s.0.data_catalog_source.0.iam_role_arns.%d", schemaExternalSchemaAttr, i), arn))
						}
						return results
					}()...),
				),
			},
		},
	})
}

// Acceptance test for external redshift schema using Hive metastore
// The following environment variables must be set, otherwise the test will be skipped:
//
//	REDSHIFT_EXTERNAL_SCHEMA_HIVE_DATABASE - source database name
//	REDSHIFT_EXTERNAL_SCHEMA_HIVE_HOSTNAME - hive metastore database endpoint FQDN or IP address
//	REDSHIFT_EXTERNAL_SCHEMA_HIVE_IAM_ROLE_ARNS - comma-separated list of ARNs to use
//
// Additionally, the following environment variables may be optionally set:
//
//	REDSHIFT_EXTERNAL_SCHEMA_HIVE_PORT - hive metastore port. Default is 9083
func TestAccDataSourceRedshiftSchema_ExternalHive(t *testing.T) {
	dbName := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_HIVE_DATABASE", t)
	dbHostname := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_HIVE_HOSTNAME", t)
	iamRoleArnsRaw := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_HIVE_IAM_ROLE_ARNS", t)
	iamRoleArns := strings.Split(iamRoleArnsRaw, ",")
	dbPort := os.Getenv("REDSHIFT_EXTERNAL_SCHEMA_HIVE_PORT")
	if dbPort == "" {
		dbPort = "9083"
	}
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_external_data_schema_hive"), "-", "_")
	configCreate := fmt.Sprintf(`
resource "redshift_schema" "hive" {
	%[1]s = %[2]q
	%[3]s {
		database_name = %[4]q
		hive_metastore_source {
			hostname = %[5]q
			port = %[6]s
			iam_role_arns = %[7]s
		}
	}
}

data "redshift_schema" "hive" {
	%[1]s = redshift_schema.hive.%[1]s
}
`,
		schemaNameAttr, schemaName, schemaExternalSchemaAttr, dbName, dbHostname, dbPort, tfArray(iamRoleArns))
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftSchemaDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftSchemaExists(schemaName),
					resource.TestCheckResourceAttr("data.redshift_schema.hive", "name", schemaName),
					resource.TestCheckResourceAttr("data.redshift_schema.hive", fmt.Sprintf("%s.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("data.redshift_schema.hive", fmt.Sprintf("%s.0.database_name", schemaExternalSchemaAttr), dbName),
					resource.TestCheckResourceAttr("data.redshift_schema.hive", fmt.Sprintf("%s.0.hive_metastore_source.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("data.redshift_schema.hive", fmt.Sprintf("%s.0.hive_metastore_source.0.hostname", schemaExternalSchemaAttr), dbHostname),
					resource.TestCheckResourceAttr("data.redshift_schema.hive", fmt.Sprintf("%s.0.hive_metastore_source.0.port", schemaExternalSchemaAttr), dbPort),
					resource.TestCheckResourceAttr("data.redshift_schema.hive", fmt.Sprintf("%s.0.hive_metastore_source.0.iam_role_arns.#", schemaExternalSchemaAttr), fmt.Sprintf("%d", len(iamRoleArns))),
					resource.ComposeTestCheckFunc(func() []resource.TestCheckFunc {
						results := []resource.TestCheckFunc{}
						for i, arn := range iamRoleArns {
							results = append(results, resource.TestCheckResourceAttr("data.redshift_schema.hive", fmt.Sprintf("%s.0.hive_metastore_source.0.iam_role_arns.%d", schemaExternalSchemaAttr, i), arn))
						}
						return results
					}()...),
				),
			},
		},
	})
}

// Acceptance test for external redshift schema using RDS Postgres
// The following environment variables must be set, otherwise the test will be skipped:
//
//	REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_DATABASE - source database name
//	REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_HOSTNAME - RDS endpoint FQDN or IP address
//	REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_IAM_ROLE_ARNS - comma-separated list of ARNs to use
//	REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_SECRET_ARN - ARN of the secret in Secrets Manager containing credentials for authenticating to RDS
//
// Additionally, the following environment variables may be optionally set:
//
//	REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_PORT - RDS port. Default is 5432
//	REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_SCHEMA - source database schema. Default is "public"
func TestAccDataSourceRedshiftSchema_ExternalRdsPostgres(t *testing.T) {
	dbName := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_DATABASE", t)
	dbHostname := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_HOSTNAME", t)
	iamRoleArnsRaw := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_IAM_ROLE_ARNS", t)
	iamRoleArns := strings.Split(iamRoleArnsRaw, ",")
	dbSecretArn := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_SECRET_ARN", t)
	dbPort := os.Getenv("REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}
	dbSchema := os.Getenv("REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_SCHEMA")
	if dbSchema == "" {
		dbSchema = "public"
	}
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_external_data_schema_rds_pg"), "-", "_")
	configCreate := fmt.Sprintf(`
resource "redshift_schema" "postgres" {
	%[1]s = %[2]q
	%[3]s {
		database_name = %[4]q
		rds_postgres_source {
			hostname = %[5]q
			port = %[6]s
			schema = %[7]q
			iam_role_arns = %[8]s
			secret_arn = %[9]q
		}
	}
}

data "redshift_schema" "postgres" {
	%[1]s = redshift_schema.postgres.%[1]s
}
`,
		schemaNameAttr, schemaName, schemaExternalSchemaAttr, dbName, dbHostname, dbPort, dbSchema, tfArray(iamRoleArns), dbSecretArn)
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftSchemaDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftSchemaExists(schemaName),
					resource.TestCheckResourceAttr("data.redshift_schema.postgres", "name", schemaName),
					resource.TestCheckResourceAttr("data.redshift_schema.postgres", fmt.Sprintf("%s.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("data.redshift_schema.postgres", fmt.Sprintf("%s.0.database_name", schemaExternalSchemaAttr), dbName),
					resource.TestCheckResourceAttr("data.redshift_schema.postgres", fmt.Sprintf("%s.0.rds_postgres_source.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("data.redshift_schema.postgres", fmt.Sprintf("%s.0.rds_postgres_source.0.hostname", schemaExternalSchemaAttr), dbHostname),
					resource.TestCheckResourceAttr("data.redshift_schema.postgres", fmt.Sprintf("%s.0.rds_postgres_source.0.port", schemaExternalSchemaAttr), dbPort),
					resource.TestCheckResourceAttr("data.redshift_schema.postgres", fmt.Sprintf("%s.0.rds_postgres_source.0.schema", schemaExternalSchemaAttr), dbSchema),
					resource.TestCheckResourceAttr("data.redshift_schema.postgres", fmt.Sprintf("%s.0.rds_postgres_source.0.secret_arn", schemaExternalSchemaAttr), dbSecretArn),
					resource.TestCheckResourceAttr("data.redshift_schema.postgres", fmt.Sprintf("%s.0.rds_postgres_source.0.iam_role_arns.#", schemaExternalSchemaAttr), fmt.Sprintf("%d", len(iamRoleArns))),
					resource.ComposeTestCheckFunc(func() []resource.TestCheckFunc {
						results := []resource.TestCheckFunc{}
						for i, arn := range iamRoleArns {
							results = append(results, resource.TestCheckResourceAttr("data.redshift_schema.postgres", fmt.Sprintf("%s.0.rds_postgres_source.0.iam_role_arns.%d", schemaExternalSchemaAttr, i), arn))
						}
						return results
					}()...),
				),
			},
		},
	})
}

// Acceptance test for external redshift schema using RDS Mysql
// The following environment variables must be set, otherwise the test will be skipped:
//
//	REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_DATABASE - source database name
//	REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_HOSTNAME - RDS endpoint FQDN or IP address
//	REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_IAM_ROLE_ARNS - comma-separated list of ARNs to use
//	REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_SECRET_ARN - ARN of the secret in Secrets Manager containing credentials for authenticating to RDS
//
// Additionally, the following environment variables may be optionally set:
//
//	REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_PORT - RDS port. Default is 3306
func TestAccDataSourceRedshiftSchema_ExternalRdsMysql(t *testing.T) {
	dbName := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_DATABASE", t)
	dbHostname := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_HOSTNAME", t)
	iamRoleArnsRaw := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_IAM_ROLE_ARNS", t)
	iamRoleArns := strings.Split(iamRoleArnsRaw, ",")
	dbSecretArn := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_SECRET_ARN", t)
	dbPort := os.Getenv("REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_PORT")
	if dbPort == "" {
		dbPort = "3306"
	}
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_external_data_schema_rds_mysql"), "-", "_")
	configCreate := fmt.Sprintf(`
resource "redshift_schema" "mysql" {
	%[1]s = %[2]q
	%[3]s {
		database_name = %[4]q
		rds_mysql_source {
			hostname = %[5]q
			port = %[6]s
			iam_role_arns = %[7]s
			secret_arn = %[8]q
		}
	}
}

data "redshift_schema" "mysql" {
	%[1]s = redshift_schema.mysql.%[1]s
}
`,
		schemaNameAttr, schemaName, schemaExternalSchemaAttr, dbName, dbHostname, dbPort, tfArray(iamRoleArns), dbSecretArn)
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftSchemaDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftSchemaExists(schemaName),
					resource.TestCheckResourceAttr("data.redshift_schema.mysql", "name", schemaName),
					resource.TestCheckResourceAttr("data.redshift_schema.mysql", fmt.Sprintf("%s.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("data.redshift_schema.mysql", fmt.Sprintf("%s.0.database_name", schemaExternalSchemaAttr), dbName),
					resource.TestCheckResourceAttr("data.redshift_schema.mysql", fmt.Sprintf("%s.0.rds_mysql_source.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("data.redshift_schema.mysql", fmt.Sprintf("%s.0.rds_mysql_source.0.hostname", schemaExternalSchemaAttr), dbHostname),
					resource.TestCheckResourceAttr("data.redshift_schema.mysql", fmt.Sprintf("%s.0.rds_mysql_source.0.port", schemaExternalSchemaAttr), dbPort),
					resource.TestCheckResourceAttr("data.redshift_schema.mysql", fmt.Sprintf("%s.0.rds_mysql_source.0.secret_arn", schemaExternalSchemaAttr), dbSecretArn),
					resource.TestCheckResourceAttr("data.redshift_schema.mysql", fmt.Sprintf("%s.0.rds_mysql_source.0.iam_role_arns.#", schemaExternalSchemaAttr), fmt.Sprintf("%d", len(iamRoleArns))),
					resource.ComposeTestCheckFunc(func() []resource.TestCheckFunc {
						results := []resource.TestCheckFunc{}
						for i, arn := range iamRoleArns {
							results = append(results, resource.TestCheckResourceAttr("data.redshift_schema.mysql", fmt.Sprintf("%s.0.rds_mysql_source.0.iam_role_arns.%d", schemaExternalSchemaAttr, i), arn))
						}
						return results
					}()...),
				),
			},
		},
	})
}

// Acceptance test for external redshift schema using datashare database
// The following environment variables must be set, otherwise the test will be skipped:
//
//	REDSHIFT_EXTERNAL_SCHEMA_REDSHIFT_DATABASE - source database name
//
// Additionally, the following environment variables may be optionally set:
//
//	REDSHIFT_EXTERNAL_SCHEMA_REDSHIFT_SCHEMA - datashare schema name. Default is "public"
func TestAccDataSourceRedshiftSchema_ExternalRedshift(t *testing.T) {
	dbName := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_REDSHIFT_DATABASE", t)
	dbSchema := os.Getenv("REDSHIFT_EXTERNAL_SCHEMA_REDSHIFT_SCHEMA")
	if dbSchema == "" {
		dbSchema = "public"
	}
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_external_data_schema_redshift"), "-", "_")
	configCreate := fmt.Sprintf(`
resource "redshift_schema" "redshift" {
	%[1]s = %[2]q
	%[3]s {
		database_name = %[4]q
		redshift_source {
			schema = %[5]q
		}
	}
}

data "redshift_schema" "redshift" {
	%[1]s = redshift_schema.redshift.%[1]s
}
`,
		schemaNameAttr, schemaName, schemaExternalSchemaAttr, dbName, dbSchema)
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftSchemaDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftSchemaExists(schemaName),
					resource.TestCheckResourceAttr("data.redshift_schema.redshift", "name", schemaName),
					resource.TestCheckResourceAttr("data.redshift_schema.redshift", fmt.Sprintf("%s.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("data.redshift_schema.redshift", fmt.Sprintf("%s.0.database_name", schemaExternalSchemaAttr), dbName),
					resource.TestCheckResourceAttr("data.redshift_schema.redshift", fmt.Sprintf("%s.0.redshift_source.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("data.redshift_schema.redshift", fmt.Sprintf("%s.0.redshift_source.0.schema", schemaExternalSchemaAttr), dbSchema),
				),
			},
		},
	})
}
