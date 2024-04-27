package redshift

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccRedshiftDatashare_Basic(t *testing.T) {
	_ = getEnvOrSkip("REDSHIFT_DATASHARE_SUPPORTED", t)
	me := strings.ToLower(permanentUsername(os.Getenv("REDSHIFT_USER")))
	shareName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_datashare_basic"), "-", "_")
	configCreate := fmt.Sprintf(`
resource "redshift_schema" "schema" {
	%[1]s = %[2]q
	%[3]s = true
}

resource "redshift_user" "user" {
	%[4]s = %[2]q
}

resource "redshift_datashare" "basic" {
	%[5]s = %[2]q
	depends_on = [
		redshift_user.user,
	]
}
`, schemaNameAttr, shareName, schemaCascadeOnDeleteAttr, userNameAttr, dataShareNameAttr)

	configUpdate := fmt.Sprintf(`
resource "redshift_schema" "schema" {
	%[1]s = %[2]q
	%[3]s = true
}

resource "redshift_user" "user" {
	%[4]s = %[2]q
}

resource "redshift_datashare" "basic" {
	%[5]s = %[2]q
	%[6]s = redshift_user.user.%[4]s
	%[7]s = true
	%[8]s = [
		redshift_schema.schema.%[1]s,
	]
}
`, schemaNameAttr, shareName, schemaCascadeOnDeleteAttr, userNameAttr, dataShareNameAttr, dataShareOwnerAttr, dataSharePublicAccessibleAttr, dataShareSchemasAttr)
	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftDatashareDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftDatashareExists(shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", dataShareNameAttr, shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", dataShareOwnerAttr, me),
					resource.TestCheckResourceAttr("redshift_datashare.basic", dataSharePublicAccessibleAttr, "false"),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", dataShareProducerAccountAttr),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", dataShareProducerNamespaceAttr),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", dataShareCreatedAttr),
					resource.TestCheckResourceAttr("redshift_datashare.basic", fmt.Sprintf("%s.#", dataShareSchemasAttr), "0"),
				),
			},
			{
				Config: configUpdate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftDatashareExists(shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", dataShareNameAttr, shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", dataShareOwnerAttr, shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", dataSharePublicAccessibleAttr, "true"),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", dataShareProducerAccountAttr),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", dataShareProducerNamespaceAttr),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", dataShareCreatedAttr),
					resource.TestCheckResourceAttr("redshift_datashare.basic", fmt.Sprintf("%s.#", dataShareSchemasAttr), "1"),
					resource.TestCheckTypeSetElemAttr("redshift_datashare.basic", fmt.Sprintf("%s.*", dataShareSchemasAttr), shareName),
				),
			},
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftDatashareExists(shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", dataShareNameAttr, shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", dataShareOwnerAttr, shareName),
					resource.TestCheckResourceAttr("redshift_datashare.basic", dataSharePublicAccessibleAttr, "false"),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", dataShareProducerAccountAttr),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", dataShareProducerNamespaceAttr),
					resource.TestCheckResourceAttrSet("redshift_datashare.basic", dataShareCreatedAttr),
					resource.TestCheckResourceAttr("redshift_datashare.basic", fmt.Sprintf("%s.#", dataShareSchemasAttr), "0"),
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

		exists, err := checkDatashareExists(client, rs.Primary.Attributes[dataShareNameAttr])

		if err != nil {
			return fmt.Errorf("Error checking datashare %w", err)
		}

		if exists {
			return fmt.Errorf("Datashare still exists after destroy")
		}
	}

	return nil
}
