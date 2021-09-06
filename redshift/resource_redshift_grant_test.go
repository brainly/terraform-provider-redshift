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
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftGrantConfig_BasicDatabase(groupName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("%s_database", groupName)),
					resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
					resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "database"),
					resource.TestCheckResourceAttr("redshift_grant.grant", "privileges.#", "2"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "create"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "temporary"),
				),
			},
		},
	})
}

func testAccRedshiftGrantConfig_BasicDatabase(groupName string) string {
	return fmt.Sprintf(`
resource "redshift_group" "group" {
  name = %[1]q
}

resource "redshift_grant" "grant" {
  group = redshift_group.group.name
  object_type = "database"
  privileges = ["create", "temporary"]
}`, groupName)
}

func TestAccRedshiftGrant_BasicSchema(t *testing.T) {
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user_basic"), "-", "_")
	groupName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group_basic"), "-", "_")
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_basic"), "-", "_")

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftGrantConfig_BasicSchema(userName, groupName, schemaName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("%s_schema_%s", groupName, schemaName)),
					resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
					resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "schema"),
					resource.TestCheckResourceAttr("redshift_grant.grant", "privileges.#", "2"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "create"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "usage"),
				),
			},
		},
	})
}

func testAccRedshiftGrantConfig_BasicSchema(userName, groupName, schemaName string) string {
	return fmt.Sprintf(`
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
`, userName, groupName, schemaName)
}

func TestAccRedshiftGrant_BasicTable(t *testing.T) {
	groupName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group_basic"), "-", "_")

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftGrantConfig_BasicTable(groupName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("%s_table_pg_catalog_pg_user_info", groupName)),
					resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
					resource.TestCheckResourceAttr("redshift_grant.grant", "schema", "pg_catalog"),
					resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "table"),
					resource.TestCheckResourceAttr("redshift_grant.grant", "objects.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "objects.*", "pg_user_info"),
					resource.TestCheckResourceAttr("redshift_grant.grant", "privileges.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "select"),
				),
			},
		},
	})
}

func testAccRedshiftGrantConfig_BasicTable(groupName string) string {
	return fmt.Sprintf(`
resource "redshift_group" "group" {
  name = %[1]q
}

resource "redshift_grant" "grant" {
  group = redshift_group.group.name
  schema = "pg_catalog"

  object_type = "table"
  objects = ["pg_user_info"]
  privileges = ["select"]
}
`, groupName)
}
