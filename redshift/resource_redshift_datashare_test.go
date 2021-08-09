package redshift

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/lib/pq"
)

func TestAccRedshiftDatashare_Basic(t *testing.T) {
	shareName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_datashare_basic"), "-", "_")
	config := fmt.Sprintf(`
resource "redshift_schema" "schema" {
	name = %[1]q
	cascade_on_delete = true
}

resource "redshift_user" "user" {
	name = %[1]q
}

resource "redshift_datashare" "basic" {
	name = %[1]q
	owner = redshift_user.user.name
	schema {
		name = redshift_schema.schema.name
		mode = "auto"
	}
}
`, shareName)
	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftDatashareDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftDatashareExists(shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "name", shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "owner", shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "publicly_accessible", "false"),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", "producer_account"),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", "producer_namespace"),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", "created"),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "schema.#", "1"),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "schema.0.name", shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "schema.0.mode", "auto"),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "schema.0.tables.#", "0"),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "schema.0.functions.#", "0"),
				),
			},
			{
				// This test step creates some dummy objects and adds them to the datashare.
				// This is done in raw SQL in the PreConfig function, for now, as the provider
				// doesn't yet have resource definitions for tables/views/functions.
				PreConfig: testAccRedshiftDatashareCreateObjects(t, shareName),
				Config:    config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftDatashareExists(shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "name", shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "owner", shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "publicly_accessible", "false"),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", "producer_account"),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", "producer_namespace"),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", "created"),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "schema.#", "1"),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "schema.0.name", shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "schema.0.mode", "auto"),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "schema.0.tables.#", "4"),
					resource.TestCheckResourceAttr("redshift_datashare.basic", "schema.0.functions.#", "1"),
				),
			},
			{
				ResourceName:      "redshift_datashare.basic",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func testAccRedshiftDatashareCreateObjects(t *testing.T, schemaName string) func() {
	return func() {
		client := testAccProvider.Meta().(*Client)
		tx, err := startTransaction(client, "")
		if err != nil {
			t.Errorf("Unable to start transaction: %w", err)
		}
		defer deferredRollback(tx)

		query := fmt.Sprintf(`
CREATE TABLE %[1]s.test_table (message varchar(max));
CREATE VIEW %[1]s.test_view AS (SELECT message FROM %[1]s.test_table);
CREATE VIEW %[1]s.test_late_binding_view AS (SELECT * FROM %[1]s.test_view) WITH NO SCHEMA BINDING;
CREATE MATERIALIZED VIEW %[1]s.test_materialized_view BACKUP NO AUTO REFRESH NO AS (SELECT message FROM %[1]s.test_table);
CREATE FUNCTION %[1]s.test_echo (varchar(max))
  RETURNS varchar(max)
STABLE
AS $$
  SELECT $1
$$ LANGUAGE sql;`, pq.QuoteIdentifier(schemaName))

		if _, err := tx.Exec(query); err != nil {
			t.Errorf("Unable to populate datashare schema objects: %w", err)
		}

		if err = tx.Commit(); err != nil {
			t.Errorf("Unable to commit transaction: %w", err)
		}
	}
}

func testAccCheckRedshiftDatashareExists(shareName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkDatashareExists(client, shareName)
		if err != nil {
			return fmt.Errorf("Error checking datashare %s", err)
		}

		if !exists {
			return fmt.Errorf("Datashare not found")
		}

		return nil
	}
}

func checkDatashareExists(client *Client, shareName string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}

	var _rez int
	err = db.QueryRow("SELECT 1 from svv_datashares WHERE share_type = 'OUTBOUND' AND share_name = $1", strings.ToLower(shareName)).Scan(&_rez)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about datashare: %w", err)
	}

	return true, nil
}

func testAccCheckRedshiftDatashareDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "redshift_datashare" {
			continue
		}

		exists, err := checkDatashareExists(client, rs.Primary.Attributes["name"])

		if err != nil {
			return fmt.Errorf("Error checking datashare %w", err)
		}

		if exists {
			return fmt.Errorf("Datashare still exists after destroy")
		}
	}

	return nil
}
