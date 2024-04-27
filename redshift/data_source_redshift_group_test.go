package redshift

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccDataSourceRedshiftGroup_basic(t *testing.T) {
	groupName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_data_basic"), "-", "_")
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_data_basic"), "-", "_")
	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftGroupDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccDataSourceRedshiftGroupConfig_basic(groupName, userName),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.redshift_group.group", groupNameAttr, groupName),
					resource.TestCheckResourceAttr("data.redshift_group.group", fmt.Sprintf("%s.#", groupUsersAttr), "1"),
					resource.TestCheckTypeSetElemAttr("data.redshift_group.group", fmt.Sprintf("%s.*", groupUsersAttr), userName),
				),
			},
		},
	})
}

func testAccDataSourceRedshiftGroupConfig_basic(groupName string, userName string) string {
	return fmt.Sprintf(`
resource "redshift_user" "user" {
	%[1]s = %[2]q
}
resource "redshift_group" "group" {
	%[3]s = %[4]q
	%[5]s = [ redshift_user.user.%[1]s ]
}

data "redshift_group" "group" {
	%[3]s = redshift_group.group.%[3]s
}
`, userNameAttr, userName, groupNameAttr, groupName, groupUsersAttr)
}
