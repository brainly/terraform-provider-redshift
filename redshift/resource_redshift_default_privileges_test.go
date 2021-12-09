package redshift

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccRedshiftDefaultPrivileges_Basic(t *testing.T) {
	groupName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group_basic"), "-", "_")
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckDefaultPrivilegesDestory(defaultPrivilegesAllSchemasID, 100, "r", groupName),
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftDefaultPrivilegesConfig_Basic(groupName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_default_privileges.simple_table", "id", fmt.Sprintf("%s_noschema_root_table", groupName)),
					resource.TestCheckResourceAttr("redshift_default_privileges.simple_table", "group", groupName),
					resource.TestCheckResourceAttr("redshift_default_privileges.simple_table", "object_type", "table"),
					resource.TestCheckResourceAttr("redshift_default_privileges.simple_table", "privileges.#", "6"),
					resource.TestCheckTypeSetElemAttr("redshift_default_privileges.simple_table", "privileges.*", "select"),
					resource.TestCheckTypeSetElemAttr("redshift_default_privileges.simple_table", "privileges.*", "update"),
					resource.TestCheckTypeSetElemAttr("redshift_default_privileges.simple_table", "privileges.*", "insert"),
					resource.TestCheckTypeSetElemAttr("redshift_default_privileges.simple_table", "privileges.*", "delete"),
					resource.TestCheckTypeSetElemAttr("redshift_default_privileges.simple_table", "privileges.*", "drop"),
					resource.TestCheckTypeSetElemAttr("redshift_default_privileges.simple_table", "privileges.*", "references"),
				),
			},
		},
	})
}

func TestAccRedshiftDefaultPrivileges_UpdateToRevoke(t *testing.T) {
	groupName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group_basic"), "-", "_")
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckDefaultPrivilegesDestory(defaultPrivilegesAllSchemasID, 100, "r", groupName),
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftDefaultPrivilegesConfig_Update_Create(groupName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_default_privileges.priv", "id", fmt.Sprintf("%s_noschema_root_table", groupName)),
					resource.TestCheckResourceAttr("redshift_default_privileges.priv", "group", groupName),
					resource.TestCheckResourceAttr("redshift_default_privileges.priv", "object_type", "table"),
					resource.TestCheckResourceAttr("redshift_default_privileges.priv", "privileges.#", "6"),
					resource.TestCheckTypeSetElemAttr("redshift_default_privileges.priv", "privileges.*", "select"),
					resource.TestCheckTypeSetElemAttr("redshift_default_privileges.priv", "privileges.*", "update"),
					resource.TestCheckTypeSetElemAttr("redshift_default_privileges.priv", "privileges.*", "insert"),
					resource.TestCheckTypeSetElemAttr("redshift_default_privileges.priv", "privileges.*", "delete"),
					resource.TestCheckTypeSetElemAttr("redshift_default_privileges.priv", "privileges.*", "drop"),
					resource.TestCheckTypeSetElemAttr("redshift_default_privileges.priv", "privileges.*", "references"),
				),
			},
			{
				Config: testAccRedshiftDefaultPrivilegesConfig_Update_Updated(groupName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_default_privileges.priv", "id", fmt.Sprintf("%s_noschema_root_table", groupName)),
					resource.TestCheckResourceAttr("redshift_default_privileges.priv", "group", groupName),
					resource.TestCheckResourceAttr("redshift_default_privileges.priv", "object_type", "table"),
					resource.TestCheckResourceAttr("redshift_default_privileges.priv", "privileges.#", "0"),
				),
			},
		},
	})
}

func testAccCheckDefaultPrivilegesDestory(schemaID, ownerID int, objectType, groupName string) func(*terraform.State) error {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		for _, rs := range s.RootModule().Resources {
			if rs.Type != "redshift_default_privileges" {
				continue
			}

			exists, err := checkDefACLExists(client, schemaID, ownerID, objectType, groupName)

			if err != nil {
				return fmt.Errorf("Error checking role %s", err)
			}

			if exists {
				return fmt.Errorf("User still exists after destroy")
			}
		}

		return nil
	}
}

func checkDefACLExists(client *Client, schemaID, ownerID int, objectType, groupName string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}

	var _rez int
	err = db.QueryRow(
		fmt.Sprintf("SELECT 1 from pg_default_acl WHERE defaclobjtype=$1 AND defaclnamespace=$2 AND defacluser=$3 AND array_to_string(defaclacl, '|') LIKE '%%group %s=%%'", groupName),
		objectType,
		schemaID,
		ownerID,
	).Scan(&_rez)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about default ACL: %s", err)
	}

	return true, nil
}

func testAccRedshiftDefaultPrivilegesConfig_Basic(groupName string) string {
	return fmt.Sprintf(`
resource "redshift_group" "group" {
  name = %[1]q
}

resource "redshift_default_privileges" "simple_table" {
  group = redshift_group.group.name
  owner = "root"
  object_type = "table"
  privileges = ["select", "update", "insert", "delete", "drop", "references"]
}`, groupName)
}

func testAccRedshiftDefaultPrivilegesConfig_Update_Create(groupName string) string {
	return fmt.Sprintf(`
resource "redshift_group" "group" {
  name = %[1]q
}

resource "redshift_default_privileges" "priv" {
  group = redshift_group.group.name
  owner = "root"
  object_type = "table"
  privileges = ["select", "update", "insert", "delete", "drop", "references"]
}`, groupName)
}

func testAccRedshiftDefaultPrivilegesConfig_Update_Updated(groupName string) string {
	return fmt.Sprintf(`
resource "redshift_group" "group" {
  name = %[1]q
}

resource "redshift_default_privileges" "priv" {
  group = redshift_group.group.name
  owner = "root"
  object_type = "table"
  privileges = []
}`, groupName)
}
