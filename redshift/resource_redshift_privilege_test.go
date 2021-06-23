package redshift

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestValidatePrivileges(t *testing.T) {
	tests := map[string]struct {
		privileges []string
		objectType string
		expected   bool
	}{
		"valid list for schema": {
			privileges: []string{"create", "usage"},
			objectType: "schema",
			expected:   true,
		},
		"invalid list for schema": {
			privileges: []string{"foo"},
			objectType: "schema",
			expected:   false,
		},
		"extended invalid list for schema": {
			privileges: []string{"create", "usage", "insert"},
			objectType: "schema",
			expected:   false,
		},
		"empty list for schema": {
			privileges: []string{},
			objectType: "schema",
			expected:   true,
		},
		"valid list for table": {
			privileges: []string{"insert", "update", "delete", "select", "references"},
			objectType: "table",
			expected:   true,
		},
		"invalid list for table": {
			privileges: []string{"foobar"},
			objectType: "schema",
			expected:   false,
		},
		"extended invalid list for table": {
			privileges: []string{"create", "usage", "insert"},
			objectType: "table",
			expected:   false,
		},
		"empty list for table": {
			privileges: []string{},
			objectType: "table",
			expected:   true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			result := validatePrivileges(tt.privileges, tt.objectType)

			if result != tt.expected {
				t.Errorf("Expected result to be `%t` but got `%t`", tt.expected, result)
			}
		})
	}
}

func TestAccRedshiftPrivilege_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftPrivilegeConfig,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_privilege.simple_table", "id", "test_simple_public_table"),
					resource.TestCheckResourceAttr("redshift_privilege.simple_table", "schema", "public"),
					resource.TestCheckResourceAttr("redshift_privilege.simple_table", "group", "test_simple"),
					resource.TestCheckResourceAttr("redshift_privilege.simple_table", "object_type", "table"),
					resource.TestCheckResourceAttr("redshift_privilege.simple_table", "privileges.#", "2"),
					resource.TestCheckTypeSetElemAttr("redshift_privilege.simple_table", "privileges.*", "select"),
					resource.TestCheckTypeSetElemAttr("redshift_privilege.simple_table", "privileges.*", "update"),

					resource.TestCheckResourceAttr("redshift_privilege.simple_schema", "id", "test_simple_public_schema"),
					resource.TestCheckResourceAttr("redshift_privilege.simple_schema", "schema", "public"),
					resource.TestCheckResourceAttr("redshift_privilege.simple_schema", "group", "test_simple"),
					resource.TestCheckResourceAttr("redshift_privilege.simple_schema", "object_type", "schema"),
					resource.TestCheckResourceAttr("redshift_privilege.simple_schema", "privileges.#", "2"),
					resource.TestCheckTypeSetElemAttr("redshift_privilege.simple_schema", "privileges.*", "create"),
					resource.TestCheckTypeSetElemAttr("redshift_privilege.simple_schema", "privileges.*", "usage"),

					resource.TestCheckResourceAttr("redshift_privilege.defaults_table", "id", "test_defaults_public_table"),
					resource.TestCheckResourceAttr("redshift_privilege.defaults_table", "schema", "public"),
					resource.TestCheckResourceAttr("redshift_privilege.defaults_table", "group", "test_defaults"),
					resource.TestCheckResourceAttr("redshift_privilege.defaults_table", "object_type", "table"),
					resource.TestCheckResourceAttr("redshift_privilege.defaults_table", "privileges.#", "0"),

					resource.TestCheckResourceAttr("redshift_privilege.test_priv_schema", "id", "test_priv_schema_test_priv_schema_schema"),
					resource.TestCheckResourceAttr("redshift_privilege.test_priv_schema", "schema", "test_priv_schema"),
					resource.TestCheckResourceAttr("redshift_privilege.test_priv_schema", "group", "test_priv_schema"),
					resource.TestCheckResourceAttr("redshift_privilege.test_priv_schema", "object_type", "schema"),
					resource.TestCheckResourceAttr("redshift_privilege.test_priv_schema", "privileges.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_privilege.test_priv_schema", "privileges.*", "usage"),
				),
			},
		},
	})
}

const testAccRedshiftPrivilegeConfig = `
resource "redshift_group" "test_simple" {
  name = "test_simple"
}

resource "redshift_privilege" "simple_table" {
  group = redshift_group.test_simple.name
  schema = "public"
  object_type = "table"
  privileges = ["select", "update"]
}

resource "redshift_privilege" "simple_schema" {
  group = redshift_group.test_simple.name
  schema = "public"
  object_type = "schema"
  privileges = ["usage", "create"]
}

resource "redshift_group" "test_defaults" {
  name = "test_defaults"
}

resource "redshift_privilege" "defaults_table" {
  group = redshift_group.test_defaults.name
  schema = "public"
  object_type = "table"
  privileges = []
}


resource "redshift_schema" "test_priv_schema" {
  name = "test_priv_schema"
}
resource "redshift_group" "test_priv_schema" {
  name = "test_priv_schema"
}
resource "redshift_privilege" "test_priv_schema" {
  group = redshift_group.test_priv_schema.name
  schema = redshift_schema.test_priv_schema.name
  object_type = "schema"
  privileges = ["usage"]
}
`
