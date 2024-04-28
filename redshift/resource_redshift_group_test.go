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

func TestAccRedshiftGroup_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftGroupDestroy,
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
	groupNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("TF_acc_group"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group@tf_acc_domain.tld"), "-", "_"),
	}
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("TF_Acc_Group_User"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}
	groupNameUpdated := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group_updated"), "-", "_")

	for _, groupName := range groupNames {
		userName1 := userNames[0]
		userName2 := userNames[1]
		userName3 := userNames[2]

		configCreate := fmt.Sprintf(`
		resource "redshift_group" "update_group" {
		  name = %[1]q
		}
		`, groupName)

		configUpdate := fmt.Sprintf(`
		resource "redshift_user" "group_update_user1" {
		  name = %[1]q
		}
		
		resource "redshift_user" "group_update_user2" {
		  name = %[2]q
		}

		resource "redshift_user" "group_update_user3" {
			name = %[3]q
		  }
		
		resource "redshift_group" "update_group" {
		  name = %[4]q
		  users = [
			redshift_user.group_update_user1.name,
			redshift_user.group_update_user2.name,
			redshift_user.group_update_user3.name,
		  ]
		}
		`, userName1, userName2, userName3, groupNameUpdated)
		resource.Test(t, resource.TestCase{
			PreCheck:          func() { testAccPreCheck(t) },
			ProviderFactories: testAccProviders,
			CheckDestroy:      testAccCheckRedshiftGroupDestroy,
			Steps: []resource.TestStep{
				{
					Config: configCreate,
					Check: resource.ComposeTestCheckFunc(
						testAccCheckRedshiftGroupExists(groupName),
						resource.TestCheckResourceAttr("redshift_group.update_group", "name", strings.ToLower(groupName)),
						resource.TestCheckResourceAttr("redshift_group.update_group", "users.#", "0"),
					),
				},
				{
					Config: configUpdate,
					Check: resource.ComposeTestCheckFunc(
						testAccCheckRedshiftGroupExists(groupNameUpdated),
						resource.TestCheckResourceAttr("redshift_group.update_group", "name", strings.ToLower(groupNameUpdated)),
						resource.TestCheckResourceAttr("redshift_group.update_group", "users.#", "3"),
						resource.TestCheckTypeSetElemAttr("redshift_group.update_group", "users.*", userName1),
						resource.TestCheckTypeSetElemAttr("redshift_group.update_group", "users.*", userName2),
						resource.TestCheckTypeSetElemAttr("redshift_group.update_group", "users.*", userName3),
					),
				},
				// apply the first one again to check if all parameters roll back properly
				{
					Config: configCreate,
					Check: resource.ComposeTestCheckFunc(
						testAccCheckRedshiftGroupExists(groupName),
						resource.TestCheckResourceAttr("redshift_group.update_group", "name", strings.ToLower(groupName)),
						resource.TestCheckResourceAttr("redshift_group.update_group", "users.#", "0"),
					),
				},
			},
		})
	}
}

func TestAccRedshiftGroup_RemoveExistingUser(t *testing.T) {
	groupName := strings.ReplaceAll(acctest.RandomWithPrefix("TF_acc_group"), "-", "_")
	userName1 := strings.ReplaceAll(acctest.RandomWithPrefix("TF_Acc_Group_User"), "-", "_")
	userName2 := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group_user"), "-", "_")

	configCreate := fmt.Sprintf(`
resource "redshift_group" "group" {
  name  = %[1]q
  users = [
	redshift_user.user1.name,
	redshift_user.user2.name,
  ]
}

resource "redshift_user" "user1" {
	name = %[2]q
}
  
resource "redshift_user" "user2" {
name = %[3]q
}
`, groupName, userName1, userName2)

	configUpdate := fmt.Sprintf(`
resource "redshift_group" "group" {
	name  = %[1]q
	users = [
		redshift_user.user1.name
	]
}

resource "redshift_user" "user1" {
	name = %[2]q
}

resource "redshift_user" "user2" {
	name = %[3]q
}
`, groupName, userName1, userName2)
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftGroupDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftGroupExists(groupName),
					resource.TestCheckResourceAttr("redshift_group.group", "name", strings.ToLower(groupName)),
					resource.TestCheckResourceAttr("redshift_group.group", "users.#", "2"),
					resource.TestCheckTypeSetElemAttr("redshift_group.group", "users.*", userName1),
					resource.TestCheckTypeSetElemAttr("redshift_group.group", "users.*", userName2),
				),
			},
			{
				Config: configUpdate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftGroupExists(groupName),
					resource.TestCheckResourceAttr("redshift_group.group", "name", strings.ToLower(groupName)),
					resource.TestCheckResourceAttr("redshift_group.group", "users.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_group.group", "users.*", userName1),
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
	err = db.QueryRow("SELECT 1 FROM pg_group WHERE groname=$1", strings.ToLower(group)).Scan(&_rez)
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
  name = "GROUP_TEST_USER1"
}

resource "redshift_user" "group_test_user2" {
  name = "group_test_user2"
}
`
