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
	basicConfig := `
	resource "redshift_role" "simple" {
		name = "role_simple"
	}
	
	resource "redshift_role" "fancy_name" {
		name = "sOme_fancy_name-@www"
	}
`

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: basicConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleExists("role_simple"),
					resource.TestCheckResourceAttr("redshift_role.simple", "name", "role_simple"),

					testAccCheckRedshiftRoleExists("sOme_fancy_name-@www"),
					resource.TestCheckResourceAttr("redshift_role.fancy_name", "name", "sOme_fancy_name-@www"),
				),
			},
		},
	})
}

func TestAccRedshiftRole_Update(t *testing.T) {
	roleNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_role"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_role"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_role@tf_acc_domain.tld"), "-", "_"),
	}
	roleNameUpdated := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_role_updated"), "-", "_")

	for _, roleName := range roleNames {
		configCreate := fmt.Sprintf(`
		resource "redshift_role" "update_role" {
		  name = %[1]q
		}
		`, roleName)

		configUpdate := fmt.Sprintf(`
		resource "redshift_role" "update_role" {
		  name = %[1]q
		}
		`, roleNameUpdated)
		resource.Test(t, resource.TestCase{
			PreCheck:     func() { testAccPreCheck(t) },
			Providers:    testAccProviders,
			CheckDestroy: testAccCheckRedshiftRoleDestroy,
			Steps: []resource.TestStep{
				{
					Config: configCreate,
					Check: resource.ComposeTestCheckFunc(
						testAccCheckRedshiftRoleExists(roleName),
						resource.TestCheckResourceAttr("redshift_role.update_role", "name", strings.ToLower(roleName)),
					),
				},
				{
					Config: configUpdate,
					Check: resource.ComposeTestCheckFunc(
						testAccCheckRedshiftRoleExists(roleNameUpdated),
						resource.TestCheckResourceAttr("redshift_role.update_role", "name", strings.ToLower(roleNameUpdated)),
					),
				},
				// apply the first one again to check if all parameters roll back properly
				{
					Config: configCreate,
					Check: resource.ComposeTestCheckFunc(
						testAccCheckRedshiftRoleExists(roleName),
						resource.TestCheckResourceAttr("redshift_role.update_role", "name", strings.ToLower(roleName)),
					),
				},
			},
		})
	}
}

func testAccCheckRedshiftRoleDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "redshift_role" {
			continue
		}

		exists, err := checkRoleExists(client, rs.Primary.ID)

		if err != nil {
			return fmt.Errorf("Error checking role %s", err)
		}

		if exists {
			return fmt.Errorf("Role still exists after destroy")
		}
	}

	return nil
}

func testAccCheckRedshiftRoleExists(roleName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkRoleExists(client, roleName)
		if err != nil {
			return fmt.Errorf("Error checking role %s", err)
		}

		if !exists {
			return fmt.Errorf("Role %s not found", roleName)
		}

		return nil
	}
}

func checkRoleExists(client *Client, roleName string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}
	var _rez int
	err = db.QueryRow("SELECT 1 from svv_roles WHERE role_name=$1", roleName).Scan(&_rez)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about role: %s", err)
	}

	return true, nil
}
