package redshift

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccRedshiftGroup_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftGroupDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftGroupConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftGroupExists("group_simple"),
					resource.TestCheckResourceAttr("redshift_group.simple", "name", "group_simple"),

					testAccCheckRedshiftGroupExists("sOme_fancy_name-@www"),
					resource.TestCheckResourceAttr("redshift_group.fancy_name", "name", "some_fancy_name-@www"),

					testAccCheckRedshiftGroupExists("group_defaults"),
					resource.TestCheckResourceAttr("redshift_group.group_defaults", "name", "group_defaults"),
					resource.TestCheckResourceAttr("redshift_group.group_defaults", "users.#", "0"),

					testAccCheckRedshiftGroupExists("group_users"),
					resource.TestCheckResourceAttr("redshift_group.group_users", "name", "group_users"),
					resource.TestCheckResourceAttr("redshift_group.group_users", "users.#", "2"),
				),
			},
		},
	})
}

func TestAccRedshiftGroup_Update(t *testing.T) {

	var configCreate = `
resource "redshift_group" "update_group" {
  name = "update_group"
}
`

	var configUpdate = `
resource "redshift_user" "group_update_user1" {
  name = "group_update_user1"
}

resource "redshift_user" "group_update_user2" {
  name = "group_update_user2"
}

resource "redshift_group" "update_group" {
  name = "update_group2"
  users = [
    redshift_user.group_update_user1.name,
    upper(redshift_user.group_update_user2.name),
  ]
}
`
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftGroupDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftGroupExists("update_group"),
					resource.TestCheckResourceAttr("redshift_group.update_group", "name", "update_group"),
					resource.TestCheckResourceAttr("redshift_group.update_group", "users.#", "0"),
				),
			},
			{
				Config: configUpdate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftGroupExists("update_group2"),
					resource.TestCheckResourceAttr("redshift_group.update_group", "name", "update_group2"),
					resource.TestCheckResourceAttr("redshift_group.update_group", "users.#", "2"),
				),
			},
			// apply the first one again to check if all parameters roll back properly
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftGroupExists("update_group"),
					resource.TestCheckResourceAttr("redshift_group.update_group", "name", "update_group"),
					resource.TestCheckResourceAttr("redshift_group.update_group", "users.#", "0"),
				),
			},
		},
	})
}

func testAccCheckRedshiftGroupDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "redshift_user" {
			continue
		}

		exists, err := checkGroupExists(client, rs.Primary.ID)

		if err != nil {
			return fmt.Errorf("Error checking role %s", err)
		}

		if exists {
			return fmt.Errorf("Group still exists after destroy")
		}
	}

	return nil
}

func testAccCheckRedshiftGroupExists(user string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkGroupExists(client, user)
		if err != nil {
			return fmt.Errorf("Error checking user %s", err)
		}

		if !exists {
			return fmt.Errorf("Group not found")
		}

		return nil
	}
}

func checkGroupExists(client *Client, group string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}
	var _rez int
	err = db.QueryRow("SELECT 1 from pg_group WHERE groname=$1", strings.ToLower(group)).Scan(&_rez)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about group: %s", err)
	}

	return true, nil
}

const testAccRedshiftGroupConfig = `
resource "redshift_group" "simple" {
  name = "group_simple"
}

resource "redshift_group" "fancy_name" {
  name = "sOme_fancy_name-@www"
}

resource "redshift_group" "group_defaults" {
  name = "group_defaults"
  users = []
}

resource "redshift_group" "group_users" {
  name = "group_users"
  users = [
    redshift_user.group_test_user1.name,
    redshift_user.group_test_user2.name,
  ]
}

resource "redshift_user" "group_test_user1" {
  name = "group_test_user1"
}

resource "redshift_user" "group_test_user2" {
  name = "group_test_user2"
}
`
