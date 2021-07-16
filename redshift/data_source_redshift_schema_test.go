package redshift

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccDataSourceRedshiftSchema_basic(t *testing.T) {
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_data_basic"), "-", "_")
	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftSchemaDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccDataSourceRedshiftSchemaConfig_basic(schemaName),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.redshift_schema.schema", schemaNameAttr, schemaName),
					resource.TestCheckResourceAttrSet("data.redshift_schema.schema", schemaOwnerAttr),
					resource.TestCheckResourceAttrSet("data.redshift_schema.schema", schemaQuotaAttr),
				),
			},
		},
	})
}

func testAccDataSourceRedshiftSchemaConfig_basic(schemaName string) string {
	return fmt.Sprintf(`
resource "redshift_schema" "schema" {
	%[1]s = %[2]q
}

data "redshift_schema" "schema" {
	%[1]s = redshift_schema.schema.%[1]s
}
`, schemaNameAttr, schemaName)
}
