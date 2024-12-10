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

func TestAccRedshiftRoleGrant_Basic(t *testing.T) {
	roleName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_role"), "-", "_")
	grantedRoleName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_granted_role"), "-", "_")

	config := fmt.Sprintf(`
resource "redshift_role" "test_role" {
  name = %[1]q
}

resource "redshift_role" "granted_role" {
  name = %[2]q
}

resource "redshift_role_grant" "grant" {
  role_to_assign = redshift_role.granted_role.name
  role           = redshift_role.test_role.name
}
`, roleName, grantedRoleName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftRoleGrantDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleGrantExists(roleName, grantedRoleName),
					resource.TestCheckResourceAttr("redshift_role_grant.grant", "role", roleName),
					resource.TestCheckResourceAttr("redshift_role_grant.grant", "role_to_assign", grantedRoleName),
				),
			},
		},
	})
}

func TestAccRedshiftRoleGrant_Update(t *testing.T) {
	roleName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_role"), "-", "_")
	initialGrantedRoleName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_initial_role"), "-", "_")
	updatedGrantedRoleName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_updated_role"), "-", "_")

	configInitial := fmt.Sprintf(`
resource "redshift_role" "test_role" {
  name = %[1]q
}

resource "redshift_role" "initial_granted_role" {
  name = %[2]q
}

resource "redshift_role_grant" "grant" {
  role_to_assign = redshift_role.initial_granted_role.name
  role           = redshift_role.test_role.name
}
`, roleName, initialGrantedRoleName)

	configUpdated := fmt.Sprintf(`
resource "redshift_role" "test_role" {
  name = %[1]q
}

resource "redshift_role" "updated_granted_role" {
  name = %[2]q
}

resource "redshift_role_grant" "grant" {
  role_to_assign = redshift_role.updated_granted_role.name
  role           = redshift_role.test_role.name
}
`, roleName, updatedGrantedRoleName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftRoleGrantDestroy,
		Steps: []resource.TestStep{
			{
				Config: configInitial,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleGrantExists(roleName, initialGrantedRoleName),
					resource.TestCheckResourceAttr("redshift_role_grant.grant", "role_to_assign", initialGrantedRoleName),
				),
			},
			{
				Config: configUpdated,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleGrantExists(roleName, updatedGrantedRoleName),
					resource.TestCheckResourceAttr("redshift_role_grant.grant", "role_to_assign", updatedGrantedRoleName),
				),
			},
		},
	})
}

func testAccCheckRedshiftRoleGrantDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "redshift_role_grant" {
			continue
		}

		role := rs.Primary.Attributes["role"]
		roleToAssign := rs.Primary.Attributes["role_to_assign"]

		exists, err := checkRoleGrantExists(client, role, roleToAssign)
		if err != nil {
			return fmt.Errorf("Error checking role grant %s -> %s: %w", role, roleToAssign, err)
		}

		if exists {
			return fmt.Errorf("Role grant %s -> %s still exists after destroy", role, roleToAssign)
		}
	}

	return nil
}

func testAccCheckRedshiftRoleGrantExists(roleName, grantedRoleName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkRoleGrantExists(client, roleName, grantedRoleName)
		if err != nil {
			return fmt.Errorf("Error checking role grant %s -> %s: %w", roleName, grantedRoleName, err)
		}

		if !exists {
			return fmt.Errorf("Role grant %s -> %s not found", roleName, grantedRoleName)
		}

		return nil
	}
}

func checkRoleGrantExists(client *Client, roleName, grantedRoleName string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}

	query := `
SELECT 1
FROM svv_role_grants
WHERE granted_role_name = $1 AND role_name = $2
`
	var exists int
	err = db.QueryRow(query, grantedRoleName, roleName).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("Error checking role grant: %w", err)
	}

	return exists == 1, nil
}
