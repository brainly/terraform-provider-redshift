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

func TestAccRedshiftRole_Basic(t *testing.T) {
	roleName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_role"), "-", "_")

	config := fmt.Sprintf(`
resource "redshift_role" "test_role" {
  name = %[1]q
}
`, roleName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleExists(roleName),
					resource.TestCheckResourceAttr("redshift_role.test_role", "name", roleName),
				),
			},
		},
	})
}

func TestAccRedshiftRole_Update(t *testing.T) {
	initialRoleName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_initial_role"), "-", "_")
	updatedRoleName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_updated_role"), "-", "_")

	configInitial := fmt.Sprintf(`
resource "redshift_role" "test_role" {
  name = %[1]q
}
`, initialRoleName)

	configUpdated := fmt.Sprintf(`
resource "redshift_role" "test_role" {
  name = %[1]q
}
`, updatedRoleName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: configInitial,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleExists(initialRoleName),
					resource.TestCheckResourceAttr("redshift_role.test_role", "name", initialRoleName),
				),
			},
			{
				Config: configUpdated,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleExists(updatedRoleName),
					resource.TestCheckResourceAttr("redshift_role.test_role", "name", updatedRoleName),
				),
			},
		},
	})
}

func testAccCheckRedshiftRoleDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "redshift_role" {
			continue
		}

		roleName := rs.Primary.Attributes["name"]

		exists, err := checkRoleExists(client, roleName)
		if err != nil {
			return fmt.Errorf("error checking role %s: %w", roleName, err)
		}

		if exists {
			return fmt.Errorf("role %s still exists after destroy", roleName)
		}
	}

	return nil
}

func testAccCheckRedshiftRoleExists(roleName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkRoleExists(client, roleName)
		if err != nil {
			return fmt.Errorf("error checking role %s: %w", roleName, err)
		}

		if !exists {
			return fmt.Errorf("role %s not found", roleName)
		}

		return nil
	}
}

func checkRoleExists(client *Client, roleName string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, fmt.Errorf("error connecting to the database: %w", err)
	}

	query := `
SELECT 1
FROM svv_roles
WHERE role_name = $1
`
	var exists int
	err = db.QueryRow(query, roleName).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("error checking role: %w", err)
	}

	return exists == 1, nil
}
