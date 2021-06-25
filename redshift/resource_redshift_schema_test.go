package redshift

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccRedshiftSchema_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftSchemaDestroy,
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
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftSchemaDestroy,
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
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftSchemaDestroy,
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
	err = db.QueryRow("SELECT 1 from pg_namespace WHERE nspname=$1", strings.ToLower(schema)).Scan(&_rez)

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
  owner = upper(redshift_user.schema_test_user1.name)
}

resource "redshift_schema" "fancy_name" {
  name = "wOoOT_I22_@tH15"
}

resource "redshift_user" "schema_test_user1" {
  name = "schema_test_user1"
}
`
