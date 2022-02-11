package redshift

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccRedshiftGrant_BasicDatabase(t *testing.T) {
	groupName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group_basic"), "-", "_")
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user_basic"), "-", "_")

	config := fmt.Sprintf(`
resource "redshift_group" "group" {
  name = %[1]q
}

resource "redshift_user" "user" {
  name = %[2]q
  password = "TestPassword123"
}

resource "redshift_grant" "grant" {
  group = redshift_group.group.name
  object_type = "database"
  privileges = ["create", "temporary"]
}

resource "redshift_grant" "grant_user" {
  user = redshift_user.user.name
  object_type = "database"
  privileges = ["temporary"]
}
`, groupName, userName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("gn:%s_ot:database", groupName)),
					resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
					resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "database"),
					resource.TestCheckResourceAttr("redshift_grant.grant", "privileges.#", "2"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "create"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "temporary"),

					resource.TestCheckResourceAttr("redshift_grant.grant_user", "id", fmt.Sprintf("un:%s_ot:database", userName)),
					resource.TestCheckResourceAttr("redshift_grant.grant_user", "user", userName),
					resource.TestCheckResourceAttr("redshift_grant.grant_user", "object_type", "database"),
					resource.TestCheckResourceAttr("redshift_grant.grant_user", "privileges.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "temporary"),
				),
			},
		},
	})
}

func TestAccRedshiftGrant_BasicSchema(t *testing.T) {
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user_basic"), "-", "_")
	groupName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group_basic"), "-", "_")
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_basic"), "-", "_")

	config := fmt.Sprintf(`
resource "redshift_user" "user" {
  name = %[1]q
}

resource "redshift_group" "group" {
  name = %[2]q
}

resource "redshift_schema" "schema" {
  name = %[3]q

  owner = redshift_user.user.name
}

resource "redshift_grant" "grant" {
  group = redshift_group.group.name
  schema = redshift_schema.schema.name

  object_type = "schema"
  privileges = ["create", "usage"]
}

resource "redshift_grant" "grant_user" {
  user = redshift_user.user.name
  schema = redshift_schema.schema.name
  
  object_type = "schema"
  privileges = ["create", "usage"]
}
`, userName, groupName, schemaName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("gn:%s_ot:schema_%s", groupName, schemaName)),
					resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
					resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "schema"),
					resource.TestCheckResourceAttr("redshift_grant.grant", "privileges.#", "2"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "create"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "usage"),

					resource.TestCheckResourceAttr("redshift_grant.grant_user", "id", fmt.Sprintf("un:%s_ot:schema_%s", userName, schemaName)),
					resource.TestCheckResourceAttr("redshift_grant.grant_user", "user", userName),
					resource.TestCheckResourceAttr("redshift_grant.grant_user", "object_type", "schema"),
					resource.TestCheckResourceAttr("redshift_grant.grant_user", "privileges.#", "2"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "create"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "usage"),
				),
			},
		},
	})
}

func TestAccRedshiftGrant_BasicTable(t *testing.T) {
	groupName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group_basic"), "-", "_")
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user_basic"), "-", "_")
	config := fmt.Sprintf(`
resource "redshift_group" "group" {
  name = %[1]q
}

resource "redshift_user" "user" {
  name = %[2]q
  password = "TestPassword123"
}

resource "redshift_grant" "grant" {
  group = redshift_group.group.name
  schema = "pg_catalog"

  object_type = "table"
  objects = ["pg_user_info"]
  privileges = ["select", "update", "insert", "delete", "drop", "references"]
}

resource "redshift_grant" "grant_user" {
  user = redshift_user.user.name
  schema = "pg_catalog"

  object_type = "table"
  objects = ["pg_user_info"]
  privileges = ["select", "update", "insert", "delete", "drop", "references"]
}
`, groupName, userName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("gn:%s_ot:table_pg_catalog_pg_user_info", groupName)),
					resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
					resource.TestCheckResourceAttr("redshift_grant.grant", "schema", "pg_catalog"),
					resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "table"),
					resource.TestCheckResourceAttr("redshift_grant.grant", "objects.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "objects.*", "pg_user_info"),
					resource.TestCheckResourceAttr("redshift_grant.grant", "privileges.#", "6"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "select"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "update"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "insert"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "delete"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "drop"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "references"),

					resource.TestCheckResourceAttr("redshift_grant.grant_user", "id", fmt.Sprintf("un:%s_ot:table_pg_catalog_pg_user_info", userName)),
					resource.TestCheckResourceAttr("redshift_grant.grant_user", "user", userName),
					resource.TestCheckResourceAttr("redshift_grant.grant_user", "schema", "pg_catalog"),
					resource.TestCheckResourceAttr("redshift_grant.grant_user", "object_type", "table"),
					resource.TestCheckResourceAttr("redshift_grant.grant_user", "objects.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "objects.*", "pg_user_info"),
					resource.TestCheckResourceAttr("redshift_grant.grant_user", "privileges.#", "6"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "select"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "update"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "insert"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "delete"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "drop"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "references"),
				),
			},
		},
	})
}

func TestAccRedshiftGrant_Regression_GH_Issue_24(t *testing.T) {
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user_grant"), "-", "_")
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_grant"), "-", "_")
	dbName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_db_grant"), "-", "_")
	config := fmt.Sprintf(`
resource "redshift_user" "user" {
  name = %[1]q
}

# Create a group named the same as user
resource "redshift_group" "group" {
  name = %[1]q
}

# Create a schema and set user as owner
resource "redshift_schema" "schema" {
  name = %[2]q

  owner = redshift_user.user.name
}

# The schema owner user will have all (create, usage) privileges on the schema
# Set only 'create' privilege to a group with the same name as user. In previous versions this would trigger a permanent diff in plan.
resource "redshift_grant" "schema" {
  group = redshift_group.group.name
  schema = redshift_schema.schema.name

  object_type = "schema"
  privileges = ["create"]
}
`, userName, schemaName, dbName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check:  resource.ComposeTestCheckFunc(),
			},
			// The 'ExpectNonEmptyPlan: false' option will fail the test if second run on the same config  will show any changes
			{
				Config:             config,
				Check:              resource.ComposeTestCheckFunc(),
				ExpectNonEmptyPlan: false,
			},
		},
	})
}

func TestAccRedshiftGrant_Regression_Issue_43(t *testing.T) {
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user_grant"), "-", "_")

	config := fmt.Sprintf(`
resource "redshift_user" "user" {
  name      = %[1]q
}

resource "redshift_group" "y_schema" {
  name  = "y_schema"
  users = [redshift_user.user.name]
}

resource "redshift_group" "y" {
  name  = "y"
  users = [redshift_user.user.name]
}

resource "redshift_schema" "x" {
  name  = "x"
  owner = redshift_user.user.name
}

resource "redshift_schema" "schema_x" {
  name  = "schema_x"
  owner = redshift_user.user.name
}

resource "redshift_grant" "grants1" {
  group       = redshift_group.y_schema.name
  schema      = redshift_schema.x.name
  object_type = "schema"
  privileges  = ["USAGE"]
}

resource "redshift_grant" "grants2" {
  group       = redshift_group.y.name
  schema      = redshift_schema.schema_x.name
  object_type = "schema"
  privileges  = ["USAGE"]
}
`, userName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check:  testAccRedshiftGrant_Regression_Issue_43_compare_ids("redshift_grant.grants1", "redshift_grant.grants2"),
			},
		},
	})
}

func testAccRedshiftGrant_Regression_Issue_43_compare_ids(addr1 string, addr2 string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs1, ok := s.RootModule().Resources[addr1]
		if !ok {
			return fmt.Errorf("Not found: %s", addr1)
		}
		rs2, ok := s.RootModule().Resources[addr2]
		if !ok {
			return fmt.Errorf("Not found: %s", addr2)
		}

		if rs1.Primary.ID == rs2.Primary.ID {
			return fmt.Errorf("Resources %s and %s have the same ID: %s", addr1, addr2, rs1.Primary.ID)
		}

		return nil
	}
}
