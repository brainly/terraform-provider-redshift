package redshift

import (
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccDataSourceRedshiftNamespace(t *testing.T) {
	uuidRegex := regexp.MustCompile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$")
	config := `
data "redshift_namespace" "namespace" {

}
`
	resource.ParallelTest(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check:  resource.TestMatchResourceAttr("data.redshift_namespace.namespace", "id", uuidRegex),
			},
		},
	})
}
