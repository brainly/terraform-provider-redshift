package redshift

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccDataSourceRedshiftDatabase_basic(t *testing.T) {
	dbName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_data_basic"), "-", "_")
	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: testAccDataSourceRedshiftDatabaseConfig_basic(dbName),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckDatabaseExists(dbName),
					resource.TestCheckResourceAttr("data.redshift_database.db", databaseNameAttr, dbName),
					resource.TestCheckResourceAttrSet("data.redshift_database.db", databaseOwnerAttr),
					resource.TestCheckResourceAttrSet("data.redshift_database.db", databaseConnLimitAttr),
					resource.TestCheckResourceAttr("data.redshift_database.db", fmt.Sprintf("%s.#", databaseDatashareSourceAttr), "0"),
				),
			},
		},
	})
}

func testAccDataSourceRedshiftDatabaseConfig_basic(dbName string) string {
	return fmt.Sprintf(`
resource "redshift_database" "db" {
	%[1]s = %[2]q
}

data "redshift_database" "db" {
	%[1]s = redshift_database.db.%[1]s 
}
	`, databaseNameAttr, dbName)
}
