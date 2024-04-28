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

func TestAccResourceRedshiftDatabase_Basic(t *testing.T) {
	dbName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_resource_basic"), "-", "_")
	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftDatabaseDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccResourceRedshiftDatabaseConfig_Basic(dbName),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckDatabaseExists(dbName),
					resource.TestCheckResourceAttr("redshift_database.db", databaseNameAttr, dbName),
					resource.TestCheckResourceAttrSet("redshift_database.db", databaseOwnerAttr),
					resource.TestCheckResourceAttrSet("redshift_database.db", databaseConnLimitAttr),
				),
			},
			{
				ResourceName:      "redshift_database.db",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func testAccResourceRedshiftDatabaseConfig_Basic(dbName string) string {
	return fmt.Sprintf(`
resource "redshift_database" "db" {
	%[1]s = %[2]q 
}
	`, databaseNameAttr, dbName)
}

func TestAccResourceRedshiftDatabase_Update(t *testing.T) {

	dbNameOriginal := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_resource_original"), "-", "_")
	dbNameNew := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_resource_update"), "-", "_")
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_resource_update"), "-", "_")

	configCreate := fmt.Sprintf(`
resource "redshift_database" "db" {
	%[1]s = %[2]q
}
`, databaseNameAttr, dbNameOriginal)

	configUpdate := fmt.Sprintf(`
resource "redshift_database" "db" {
	%[1]s = %[2]q
	%[3]s = redshift_user.user.%[4]s
	%[5]s = 0
}

resource "redshift_user" "user" {
	%[4]s = %[6]q
}
	`, databaseNameAttr, dbNameNew, databaseOwnerAttr, userNameAttr, databaseConnLimitAttr, userName)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftDatabaseDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckDatabaseExists(dbNameOriginal),
					resource.TestCheckResourceAttr("redshift_database.db", databaseNameAttr, dbNameOriginal),
					resource.TestCheckResourceAttrSet("redshift_database.db", databaseOwnerAttr),
					resource.TestCheckResourceAttrSet("redshift_database.db", databaseConnLimitAttr),
				),
			},
			{
				Config: configUpdate,
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckDatabaseExists(dbNameNew),
					resource.TestCheckResourceAttr("redshift_database.db", databaseNameAttr, dbNameNew),
					resource.TestCheckResourceAttr("redshift_database.db", databaseOwnerAttr, userName),
					resource.TestCheckResourceAttr("redshift_database.db", databaseConnLimitAttr, "0"),
				),
			},
		},
	})
}

func testAccResourceRedshiftDatabaseConfig_basic(dbName string) string {
	return fmt.Sprintf(`
resource "redshift_database" "db" {
	%[1]s = %[2]q
}
	`, databaseNameAttr, dbName)
}

func testAccCheckRedshiftDatabaseDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "redshift_database" {
			continue
		}

		exists, err := checkDatabaseExists(client, rs.Primary.ID)

		if err != nil {
			return fmt.Errorf("Error checking database %s", err)
		}

		if exists {
			return fmt.Errorf("Database still exists after destroy")
		}
	}

	return nil
}

func testAccCheckDatabaseExists(dbName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkDatabaseExists(client, dbName)
		if err != nil {
			return fmt.Errorf("Error checking database %w", err)
		}

		if !exists {
			return fmt.Errorf("Database not found")
		}

		return nil
	}
}

func checkDatabaseExists(client *Client, database string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}
	var _rez int
	err = db.QueryRow("SELECT 1 FROM pg_database WHERE datname=$1", strings.ToLower(database)).Scan(&_rez)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about database: %s", err)
	}

	return true, nil
}
