package redshift

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccDataSourceRedshiftUser_Basic(t *testing.T) {
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_data_user_basic"), "-", "_")
	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftUserDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccDataSourceRedshiftUserConfig_Basic(userName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftUserExists(userName),
					resource.TestCheckResourceAttr("data.redshift_user.simple", userNameAttr, userName),
					resource.TestCheckResourceAttrSet("data.redshift_user.simple", userValidUntilAttr),
					resource.TestCheckResourceAttrSet("data.redshift_user.simple", userCreateDBAttr),
					resource.TestCheckResourceAttrSet("data.redshift_user.simple", userConnLimitAttr),
					resource.TestCheckResourceAttrSet("data.redshift_user.simple", userSyslogAccessAttr),
					resource.TestCheckResourceAttrSet("data.redshift_user.simple", userSuperuserAttr),
					resource.TestCheckResourceAttrSet("data.redshift_user.simple", userSessionTimeoutAttr),
				),
			},
		},
	})
}

func testAccDataSourceRedshiftUserConfig_Basic(userName string) string {
	return fmt.Sprintf(`
resource "redshift_user" "simple" {
  %[1]s = %[2]q
}

data "redshift_user" "simple" {
	%[1]s = redshift_user.simple.%[1]s
}
`, userNameAttr, userName)
}
