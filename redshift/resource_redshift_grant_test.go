package redshift

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccRedshiftGrant_BasicDatabase(t *testing.T) {
	groupName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group_basic"), "-", "_")
	//dbName := os.Getenv("REDSHIFT_DATABASE")

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftGrantConfig_BasicDatabase(groupName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("%s_database", groupName)),
					resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
					resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "database"),
					resource.TestCheckResourceAttr("redshift_grant.grant", "privileges.#", "2"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "create"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "temporary"),
				),
			},
		},
	})
}

func testAccRedshiftGrantConfig_BasicDatabase(groupName string) string {
	return fmt.Sprintf(`
resource "redshift_group" "group" {
  name = %[1]q
}

resource "redshift_grant" "grant" {
  group = redshift_group.group.name
  object_type = "database"
  privileges = ["create", "temporary"]
}`, groupName)
}
