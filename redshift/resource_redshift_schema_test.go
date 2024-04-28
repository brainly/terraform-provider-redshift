package redshift

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccRedshiftSchema_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftSchemaDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftSchemaConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftSchemaExists("schema_simple"),
					resource.TestCheckResourceAttr("redshift_schema.simple", "name", "schema_simple"),

					testAccCheckRedshiftSchemaExists("schema_defaults"),
					resource.TestCheckResourceAttr("redshift_schema.schema_defaults", "name", "schema_defaults"),
					resource.TestCheckResourceAttr("redshift_schema.schema_defaults", "quota", "0"),
					resource.TestCheckResourceAttr("redshift_schema.schema_defaults", "cascade_on_delete", "false"),

					testAccCheckRedshiftSchemaExists("schema_configured"),
					resource.TestCheckResourceAttr("redshift_schema.schema_configured", "name", "schema_configured"),
					resource.TestCheckResourceAttr("redshift_schema.schema_configured", "quota", "15360"),
					resource.TestCheckResourceAttr("redshift_schema.schema_configured", "cascade_on_delete", "false"),

					testAccCheckRedshiftSchemaExists("wOoOT_I22_@tH15"),
					resource.TestCheckResourceAttr("redshift_schema.fancy_name", "name", "wooot_i22_@th15"),
				),
			},
		},
	})
}

func TestAccRedshiftSchema_Update(t *testing.T) {

	var configCreate = `
resource "redshift_schema" "update_schema" {
  name = "update_schema"
  owner = redshift_user.schema_user1.name
}

resource "redshift_user" "schema_user1" {
  name = "schema_user1"
}
`

	var configUpdate = `
resource "redshift_schema" "update_schema" {
  name = "update_schema2"
  quota = 10
}

resource "redshift_user" "schema_user1" {
  name = "schema_user1"
}
`
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftSchemaDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftSchemaExists("update_schema"),
					resource.TestCheckResourceAttr("redshift_schema.update_schema", "name", "update_schema"),
					resource.TestCheckResourceAttr("redshift_schema.update_schema", "quota", "0"),
				),
			},
			{
				Config: configUpdate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftSchemaExists("update_schema2"),
					resource.TestCheckResourceAttr("redshift_schema.update_schema", "name", "update_schema2"),
					resource.TestCheckResourceAttr("redshift_schema.update_schema", "quota", "10240"),
				),
			},
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftSchemaExists("update_schema"),
					resource.TestCheckResourceAttr("redshift_schema.update_schema", "name", "update_schema"),
					resource.TestCheckResourceAttr("redshift_schema.update_schema", "quota", "0"),
				),
			},
		},
	})
}

func TestAccRedshiftSchema_UpdateComplex(t *testing.T) {
	var configCreate = `
resource "redshift_schema" "update_dl_schema" {
  name = "update_dl_schema"
}
`

	var configUpdate = `
resource "redshift_schema" "update_dl_schema" {
  name = "update_dl_schema2"
  quota = 10
  owner = redshift_user.schema_dl_user1.name
}

resource "redshift_user" "schema_dl_user1" {
  name = "schema_dl_user1"
}
`
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftSchemaDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftSchemaExists("update_dl_schema"),
					resource.TestCheckResourceAttr("redshift_schema.update_dl_schema", "name", "update_dl_schema"),
					resource.TestCheckResourceAttr("redshift_schema.update_dl_schema", "quota", "0"),
				),
			},
			{
				Config: configUpdate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftSchemaExists("update_dl_schema2"),
					resource.TestCheckResourceAttr("redshift_schema.update_dl_schema", "name", "update_dl_schema2"),
					resource.TestCheckResourceAttr("redshift_schema.update_dl_schema", "quota", "10240"),
				),
			},
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftSchemaExists("update_dl_schema"),
					resource.TestCheckResourceAttr("redshift_schema.update_dl_schema", "name", "update_dl_schema"),
					resource.TestCheckResourceAttr("redshift_schema.update_dl_schema", "quota", "0"),
				),
			},
		},
	})
}

// Acceptance test for external redshift schema using AWS Glue Data Catalog
// The following environment variables must be set, otherwise the test will be skipped:
//
//	REDSHIFT_EXTERNAL_SCHEMA_DATA_CATALOG_DATABASE - source database name
//	REDSHIFT_EXTERNAL_SCHEMA_DATA_CATALOG_IAM_ROLE_ARNS - comma-separated list of ARNs to use
func TestAccRedshiftSchema_ExternalDataCatalog(t *testing.T) {
	dbName := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_DATA_CATALOG_DATABASE", t)
	iamRoleArnsRaw := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_DATA_CATALOG_IAM_ROLE_ARNS", t)
	iamRoleArns, err := splitCsvAndTrim(iamRoleArnsRaw)
	if err != nil {
		t.Errorf("REDSHIFT_EXTERNAL_SCHEMA_DATA_CATALOG_IAM_ROLE_ARNS could not be parsed: %v", err)
	}
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_external_schema_data_catalog"), "-", "_")
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
					resource.TestCheckResourceAttr("redshift_schema.spectrum", "name", schemaName),
					resource.TestCheckResourceAttr("redshift_schema.spectrum", fmt.Sprintf("%s.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("redshift_schema.spectrum", fmt.Sprintf("%s.0.database_name", schemaExternalSchemaAttr), dbName),
					resource.TestCheckResourceAttr("redshift_schema.spectrum", fmt.Sprintf("%s.0.data_catalog_source.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("redshift_schema.spectrum", fmt.Sprintf("%s.0.data_catalog_source.0.iam_role_arns.#", schemaExternalSchemaAttr), fmt.Sprintf("%d", len(iamRoleArns))),
					resource.ComposeTestCheckFunc(func() []resource.TestCheckFunc {
						results := []resource.TestCheckFunc{}
						for i, arn := range iamRoleArns {
							results = append(results, resource.TestCheckResourceAttr("redshift_schema.spectrum", fmt.Sprintf("%s.0.data_catalog_source.0.iam_role_arns.%d", schemaExternalSchemaAttr, i), arn))
						}
						return results
					}()...),
				),
			},
			{
				ResourceName:      "redshift_schema.spectrum",
				ImportState:       true,
				ImportStateVerify: true,
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
func TestAccRedshiftSchema_ExternalHive(t *testing.T) {
	dbName := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_HIVE_DATABASE", t)
	dbHostname := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_HIVE_HOSTNAME", t)
	iamRoleArnsRaw := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_HIVE_IAM_ROLE_ARNS", t)
	iamRoleArns, err := splitCsvAndTrim(iamRoleArnsRaw)
	if err != nil {
		t.Errorf("REDSHIFT_EXTERNAL_SCHEMA_DATA_CATALOG_IAM_ROLE_ARNS could not be parsed: %v", err)
	}
	dbPort := os.Getenv("REDSHIFT_EXTERNAL_SCHEMA_HIVE_PORT")
	if dbPort == "" {
		dbPort = "9083"
	}
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_external_schema_hive"), "-", "_")
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
					resource.TestCheckResourceAttr("redshift_schema.hive", "name", schemaName),
					resource.TestCheckResourceAttr("redshift_schema.hive", fmt.Sprintf("%s.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("redshift_schema.hive", fmt.Sprintf("%s.0.database_name", schemaExternalSchemaAttr), dbName),
					resource.TestCheckResourceAttr("redshift_schema.hive", fmt.Sprintf("%s.0.hive_metastore_source.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("redshift_schema.hive", fmt.Sprintf("%s.0.hive_metastore_source.0.hostname", schemaExternalSchemaAttr), dbHostname),
					resource.TestCheckResourceAttr("redshift_schema.hive", fmt.Sprintf("%s.0.hive_metastore_source.0.port", schemaExternalSchemaAttr), dbPort),
					resource.TestCheckResourceAttr("redshift_schema.hive", fmt.Sprintf("%s.0.hive_metastore_source.0.iam_role_arns.#", schemaExternalSchemaAttr), fmt.Sprintf("%d", len(iamRoleArns))),
					resource.ComposeTestCheckFunc(func() []resource.TestCheckFunc {
						results := []resource.TestCheckFunc{}
						for i, arn := range iamRoleArns {
							results = append(results, resource.TestCheckResourceAttr("redshift_schema.hive", fmt.Sprintf("%s.0.hive_metastore_source.0.iam_role_arns.%d", schemaExternalSchemaAttr, i), arn))
						}
						return results
					}()...),
				),
			},
			{
				ResourceName:      "redshift_schema.hive",
				ImportState:       true,
				ImportStateVerify: true,
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
func TestAccRedshiftSchema_ExternalRdsPostgres(t *testing.T) {
	dbName := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_DATABASE", t)
	dbHostname := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_HOSTNAME", t)
	iamRoleArnsRaw := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_IAM_ROLE_ARNS", t)
	iamRoleArns, err := splitCsvAndTrim(iamRoleArnsRaw)
	if err != nil {
		t.Errorf("REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_IAM_ROLE_ARNS could not be parsed: %v", err)
	}
	dbSecretArn := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_SECRET_ARN", t)
	dbPort := os.Getenv("REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}
	dbSchema := os.Getenv("REDSHIFT_EXTERNAL_SCHEMA_RDS_POSTGRES_SCHEMA")
	if dbSchema == "" {
		dbSchema = "public"
	}
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_external_schema_rds_pg"), "-", "_")
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
					resource.TestCheckResourceAttr("redshift_schema.postgres", "name", schemaName),
					resource.TestCheckResourceAttr("redshift_schema.postgres", fmt.Sprintf("%s.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("redshift_schema.postgres", fmt.Sprintf("%s.0.database_name", schemaExternalSchemaAttr), dbName),
					resource.TestCheckResourceAttr("redshift_schema.postgres", fmt.Sprintf("%s.0.rds_postgres_source.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("redshift_schema.postgres", fmt.Sprintf("%s.0.rds_postgres_source.0.hostname", schemaExternalSchemaAttr), dbHostname),
					resource.TestCheckResourceAttr("redshift_schema.postgres", fmt.Sprintf("%s.0.rds_postgres_source.0.port", schemaExternalSchemaAttr), dbPort),
					resource.TestCheckResourceAttr("redshift_schema.postgres", fmt.Sprintf("%s.0.rds_postgres_source.0.schema", schemaExternalSchemaAttr), dbSchema),
					resource.TestCheckResourceAttr("redshift_schema.postgres", fmt.Sprintf("%s.0.rds_postgres_source.0.secret_arn", schemaExternalSchemaAttr), dbSecretArn),
					resource.TestCheckResourceAttr("redshift_schema.postgres", fmt.Sprintf("%s.0.rds_postgres_source.0.iam_role_arns.#", schemaExternalSchemaAttr), fmt.Sprintf("%d", len(iamRoleArns))),
					resource.ComposeTestCheckFunc(func() []resource.TestCheckFunc {
						results := []resource.TestCheckFunc{}
						for i, arn := range iamRoleArns {
							results = append(results, resource.TestCheckResourceAttr("redshift_schema.postgres", fmt.Sprintf("%s.0.rds_postgres_source.0.iam_role_arns.%d", schemaExternalSchemaAttr, i), arn))
						}
						return results
					}()...),
				),
			},
			{
				ResourceName:      "redshift_schema.postgres",
				ImportState:       true,
				ImportStateVerify: true,
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
func TestAccRedshiftSchema_ExternalRdsMysql(t *testing.T) {
	dbName := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_DATABASE", t)
	dbHostname := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_HOSTNAME", t)
	iamRoleArnsRaw := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_IAM_ROLE_ARNS", t)
	iamRoleArns, err := splitCsvAndTrim(iamRoleArnsRaw)
	if err != nil {
		t.Errorf("REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_IAM_ROLE_ARNS could not be parsed: %v", err)
	}
	dbSecretArn := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_SECRET_ARN", t)
	dbPort := os.Getenv("REDSHIFT_EXTERNAL_SCHEMA_RDS_MYSQL_PORT")
	if dbPort == "" {
		dbPort = "3306"
	}
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_external_schema_rds_mysql"), "-", "_")
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
					resource.TestCheckResourceAttr("redshift_schema.mysql", "name", schemaName),
					resource.TestCheckResourceAttr("redshift_schema.mysql", fmt.Sprintf("%s.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("redshift_schema.mysql", fmt.Sprintf("%s.0.database_name", schemaExternalSchemaAttr), dbName),
					resource.TestCheckResourceAttr("redshift_schema.mysql", fmt.Sprintf("%s.0.rds_mysql_source.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("redshift_schema.mysql", fmt.Sprintf("%s.0.rds_mysql_source.0.hostname", schemaExternalSchemaAttr), dbHostname),
					resource.TestCheckResourceAttr("redshift_schema.mysql", fmt.Sprintf("%s.0.rds_mysql_source.0.port", schemaExternalSchemaAttr), dbPort),
					resource.TestCheckResourceAttr("redshift_schema.mysql", fmt.Sprintf("%s.0.rds_mysql_source.0.secret_arn", schemaExternalSchemaAttr), dbSecretArn),
					resource.TestCheckResourceAttr("redshift_schema.mysql", fmt.Sprintf("%s.0.rds_mysql_source.0.iam_role_arns.#", schemaExternalSchemaAttr), fmt.Sprintf("%d", len(iamRoleArns))),
					resource.ComposeTestCheckFunc(func() []resource.TestCheckFunc {
						results := []resource.TestCheckFunc{}
						for i, arn := range iamRoleArns {
							results = append(results, resource.TestCheckResourceAttr("redshift_schema.mysql", fmt.Sprintf("%s.0.rds_mysql_source.0.iam_role_arns.%d", schemaExternalSchemaAttr, i), arn))
						}
						return results
					}()...),
				),
			},
			{
				ResourceName:      "redshift_schema.mysql",
				ImportState:       true,
				ImportStateVerify: true,
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
func TestAccRedshiftSchema_ExternalRedshift(t *testing.T) {
	dbName := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_REDSHIFT_DATABASE", t)
	dbSchema := os.Getenv("REDSHIFT_EXTERNAL_SCHEMA_REDSHIFT_SCHEMA")
	if dbSchema == "" {
		dbSchema = "public"
	}
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_external_schema_redshift"), "-", "_")
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
					resource.TestCheckResourceAttr("redshift_schema.redshift", "name", schemaName),
					resource.TestCheckResourceAttr("redshift_schema.redshift", fmt.Sprintf("%s.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("redshift_schema.redshift", fmt.Sprintf("%s.0.database_name", schemaExternalSchemaAttr), dbName),
					resource.TestCheckResourceAttr("redshift_schema.redshift", fmt.Sprintf("%s.0.redshift_source.#", schemaExternalSchemaAttr), "1"),
					resource.TestCheckResourceAttr("redshift_schema.redshift", fmt.Sprintf("%s.0.redshift_source.0.schema", schemaExternalSchemaAttr), dbSchema),
				),
			},
			{
				ResourceName:      "redshift_schema.redshift",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func TestAccRedshiftSchema_CreateExternalDatabaseIfNotExists(t *testing.T) {
	roleArn := getEnvOrSkip("REDSHIFT_EXTERNAL_SCHEMA_IAM_ROLE_ARN", t)
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_external_schema_redshift"), "-", "_")
	dbName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_external_schema_redshift"), "-", "_")
	configCreate := fmt.Sprintf(`
resource "redshift_schema" "redshift" {
	name = %[1]q
	external_schema {
		database_name = %[2]q
		data_catalog_source {
		  iam_role_arns = [%[3]q]
		  create_external_database_if_not_exists = true
		}
	}
}
`, schemaName, dbName, roleArn)

	configUpdate := fmt.Sprintf(`
resource "redshift_schema" "redshift" {
	name = %[1]q
	external_schema {
		database_name = %[2]q
		data_catalog_source {
		  iam_role_arns = [%[3]q]
		}
	}
}
`, schemaName, dbName, roleArn)

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftSchemaDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftSchemaExists(schemaName),
					resource.TestCheckResourceAttr("redshift_schema.redshift", "name", schemaName),
				),
			},
			// Run the same config with 'ExpectNonEmptyPlan: false' to check for any constant-drift params
			{
				Config:             configCreate,
				Check:              resource.ComposeTestCheckFunc(),
				ExpectNonEmptyPlan: false,
			},
			// When "create_external_database_if_not_exists" is removed, DiffSuppressFunc will kick-in and there should be no diff in plan
			{
				Config: configUpdate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftSchemaExists(schemaName),
					resource.TestCheckResourceAttr("redshift_schema.redshift", "name", schemaName),
				),
				ExpectNonEmptyPlan: false,
			},
			// Run the same config with 'ExpectNonEmptyPlan: false' to check for any constant-drift params
			{
				Config:             configCreate,
				Check:              resource.ComposeTestCheckFunc(),
				ExpectNonEmptyPlan: false,
			},
		},
	})
}

func testAccCheckRedshiftSchemaDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "redshift_schema" {
			continue
		}

		exists, err := checkSchemaExists(client, rs.Primary.ID)

		if err != nil {
			return fmt.Errorf("Error checking schema %s", err)
		}

		if exists {
			return fmt.Errorf("Schema still exists after destroy")
		}
	}

	return nil
}

func testAccCheckRedshiftSchemaExists(schema string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkSchemaExists(client, schema)
		if err != nil {
			return fmt.Errorf("Error checking schema %s", err)
		}

		if !exists {
			return fmt.Errorf("Schema not found")
		}

		return nil
	}
}

func checkSchemaExists(client *Client, schema string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}

	var _rez int
	err = db.QueryRow("SELECT 1 FROM pg_namespace WHERE nspname=$1", strings.ToLower(schema)).Scan(&_rez)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about schema: %s", err)
	}

	return true, nil
}

const testAccRedshiftSchemaConfig = `
resource "redshift_schema" "simple" {
  name = "schema_simple"
}

resource "redshift_schema" "schema_defaults" {
  name = "schema_defaults"
  quota = 0
  cascade_on_delete = false
}

resource "redshift_schema" "schema_configured" {
  name = "schema_configured"
  quota = 15
  cascade_on_delete = false
  owner = redshift_user.schema_test_user1.name
}

resource "redshift_schema" "fancy_name" {
  name = "wOoOT_I22_@tH15"
}

resource "redshift_user" "schema_test_user1" {
  name = "schema_test_user1"
}
`
