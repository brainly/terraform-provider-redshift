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

func TestAccRedshiftDatasharePrivilege_Namespace(t *testing.T) {
	_ = getEnvOrSkip("REDSHIFT_DATASHARE_SUPPORTED", t)
	consumerNamespace := getEnvOrSkip("REDSHIFT_DATASHARE_CONSUMER_NAMESPACE", t)
	shareName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_datashare_privilege_namespace"), "-", "_")
	config := fmt.Sprintf(`
resource "redshift_datashare" "share" {
	%[1]s = %[2]q
}

resource "redshift_datashare_privilege" "consumer_namespace" {
	%[3]s = redshift_datashare.share.%[1]s
	%[4]s = %[5]q
}
`, dataShareNameAttr, shareName, datasharePrivilegeShareNameAttr, datasharePrivilegeNamespaceAttr, consumerNamespace)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftDatasharePrivilegeDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftDatashareNamespacePrivilegeExists(shareName, consumerNamespace),
					resource.TestCheckResourceAttrSet("redshift_datashare_privilege.consumer_namespace", datasharePrivilegeShareDateAttr),
				),
			},
		},
	})
}

func TestAccRedshiftDatasharePrivilege_Account(t *testing.T) {
	_ = getEnvOrSkip("REDSHIFT_DATASHARE_SUPPORTED", t)
	consumerAccount := getEnvOrSkip("REDSHIFT_DATASHARE_CONSUMER_ACCOUNT", t)
	shareName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_datashare_privilege_account"), "-", "_")
	config := fmt.Sprintf(`
resource "redshift_datashare" "share" {
	%[1]s = %[2]q
}

resource "redshift_datashare_privilege" "consumer_account" {
	%[3]s = redshift_datashare.share.%[1]s
	%[4]s = %[5]q
}
`, dataShareNameAttr, shareName, datasharePrivilegeShareNameAttr, datasharePrivilegeAccountAttr, consumerAccount)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftDatasharePrivilegeDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftDatashareAccountPrivilegeExists(shareName, consumerAccount),
					resource.TestCheckResourceAttrSet("redshift_datashare_privilege.consumer_account", datasharePrivilegeShareDateAttr),
				),
			},
		},
	})
}

func testAccCheckRedshiftDatasharePrivilegeDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "redshift_datashare_privilege" {
			continue
		}

		var exists bool
		var err error

		shareName := rs.Primary.Attributes[datasharePrivilegeShareNameAttr]

		if account, ok := rs.Primary.Attributes[datasharePrivilegeAccountAttr]; ok {
			exists, err = checkDatasharePrivilegeAccountExists(client, shareName, account)
		} else if namespace, ok := rs.Primary.Attributes[datasharePrivilegeNamespaceAttr]; ok {
			exists, err = checkDatasharePrivilegeNamespaceExists(client, shareName, namespace)
		} else {
			err = fmt.Errorf("Neither %s nor %s was set", datasharePrivilegeAccountAttr, datasharePrivilegeNamespaceAttr)
		}

		if err != nil {
			return fmt.Errorf("Error checking datashare privilege: %w", err)
		}

		if exists {
			return fmt.Errorf("Datashare privilege still exists after destroy")
		}
	}
	return nil
}

func checkDatasharePrivilegeAccountExists(client *Client, shareName string, account string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}

	var _rez int
	err = db.QueryRow("SELECT 1 from svv_datashare_consumers WHERE share_name = $1 AND consumer_account = $2", strings.ToLower(shareName), account).Scan(&_rez)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about datashare privileges: %w", err)
	}

	return true, nil
}

func checkDatasharePrivilegeNamespaceExists(client *Client, shareName string, namespace string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}

	var _rez int
	err = db.QueryRow("SELECT 1 from svv_datashare_consumers WHERE share_name = $1 AND consumer_namespace = $2", strings.ToLower(shareName), namespace).Scan(&_rez)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about datashare privileges: %w", err)
	}

	return true, nil
}

func testAccCheckRedshiftDatashareAccountPrivilegeExists(shareName string, account string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkDatasharePrivilegeAccountExists(client, shareName, account)
		if err != nil {
			return fmt.Errorf("Error checking datashare privilege %s", err)
		}

		if !exists {
			return fmt.Errorf("Datashare privilege not found")
		}

		return nil
	}
}

func testAccCheckRedshiftDatashareNamespacePrivilegeExists(shareName string, namespace string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkDatasharePrivilegeNamespaceExists(client, shareName, namespace)
		if err != nil {
			return fmt.Errorf("Error checking datashare privilege %s", err)
		}

		if !exists {
			return fmt.Errorf("Datashare privilege not found")
		}

		return nil
	}
}
