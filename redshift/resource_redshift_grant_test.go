package redshift

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/lib/pq"
)

func TestAccRedshiftGrant_SchemaToPublic(t *testing.T) {
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_schema"), "-", "_")
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_user"), "-", "_")
	config := fmt.Sprintf(`
resource "redshift_schema" "test" {
	name = %[1]q
}

resource "redshift_grant" "public" {
	group = "PUBLIC"

	schema = %[1]q
	object_type = "schema"
	privileges  = ["create", "usage", "alter"]
}

# Add user with different privileges to see if we do not catch them by accident
resource "redshift_user" "test" {
	name = %[2]q
	password = "Foo123456$"
}
resource "redshift_grant" "user" {
	user = redshift_user.test.name
	schema = %[1]q
	object_type = "schema"
	privileges  = ["usage"]
}
`, schemaName, userName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.public", "id", fmt.Sprintf("gn:public_ot:schema_%s", schemaName)),
					resource.TestCheckResourceAttr("redshift_grant.public", "group", "public"),
					resource.TestCheckResourceAttr("redshift_grant.public", "object_type", "schema"),
					resource.TestCheckResourceAttr("redshift_grant.public", "privileges.#", "2"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "create"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "usage"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "alter"),
				),
			},
		},
	})
}

func TestAccRedshiftGrant_DatabaseToPublic(t *testing.T) {
	config := `
resource "redshift_grant" "public" {
	group = "public"
	object_type = "database"
	privileges = ["temporary"]
}
`
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.public", "id", "gn:public_ot:database"),
					resource.TestCheckResourceAttr("redshift_grant.public", "group", "public"),
					resource.TestCheckResourceAttr("redshift_grant.public", "object_type", "database"),
					resource.TestCheckResourceAttr("redshift_grant.public", "privileges.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "temporary"),
				),
			},
		},
	})
}

func TestAccRedshiftGrant_LanguageToPublic(t *testing.T) {
	config := `
resource "redshift_grant" "public" {
	group = "public"
	object_type = "language"
	objects = ["plpythonu"]
	privileges = ["usage"]
}
`
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.public", "id", "gn:public_ot:language_plpythonu"),
					resource.TestCheckResourceAttr("redshift_grant.public", "group", "public"),
					resource.TestCheckResourceAttr("redshift_grant.public", "object_type", "language"),
					resource.TestCheckResourceAttr("redshift_grant.public", "privileges.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "usage"),
				),
			},
		},
	})
}

func TestAccRedshiftGrant_TableToPublic(t *testing.T) {
	config := `
resource "redshift_grant" "public" {
	group = "public"

	schema = "pg_catalog"
	object_type = "table"
	objects = ["pg_user_info"]
	privileges = ["select", "update", "insert", "delete", "drop", "references", "rule", "trigger", "alter", "truncate"]
}
`
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.public", "id", "gn:public_ot:table_pg_catalog_pg_user_info"),
					resource.TestCheckResourceAttr("redshift_grant.public", "group", "public"),
					resource.TestCheckResourceAttr("redshift_grant.public", "schema", "pg_catalog"),
					resource.TestCheckResourceAttr("redshift_grant.public", "object_type", "table"),
					resource.TestCheckResourceAttr("redshift_grant.public", "objects.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "objects.*", "pg_user_info"),
					resource.TestCheckResourceAttr("redshift_grant.public", "privileges.#", "8"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "select"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "update"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "insert"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "delete"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "drop"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "references"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "rule"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "trigger"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "truncate"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "alter"),
				),
			},
		},
	})
}

func TestAccRedshiftGrant_BasicDatabase(t *testing.T) {
	groupNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group@tf_acc_domain.tld"), "-", "_"),
	}
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}

	for i, groupName := range groupNames {
		userName := userNames[i]
		config := fmt.Sprintf(`
		resource "redshift_group" "group" {
		  name = %[1]q
		}
		
		resource "redshift_user" "user" {
		  name = %[2]q
		  password = "TestPassword123"
		}
		
		resource "redshift_grant" "grant" {
		  group = redshift_group.group.name
		  object_type = "database"
		  privileges = ["create", "temporary"]
		}
		
		resource "redshift_grant" "grant_user" {
		  user = redshift_user.user.name
		  object_type = "database"
		  privileges = ["temporary"]
		}
		`, groupName, userName)
		resource.Test(t, resource.TestCase{
			PreCheck:     func() { testAccPreCheck(t) },
			Providers:    testAccProviders,
			CheckDestroy: func(s *terraform.State) error { return nil },
			Steps: []resource.TestStep{
				{
					Config: config,
					Check: resource.ComposeTestCheckFunc(
						resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("gn:%s_ot:database", groupName)),
						resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
						resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "database"),
						resource.TestCheckResourceAttr("redshift_grant.grant", "privileges.#", "2"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "create"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "temporary"),

						resource.TestCheckResourceAttr("redshift_grant.grant_user", "id", fmt.Sprintf("un:%s_ot:database", userName)),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "user", userName),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "object_type", "database"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "privileges.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "temporary"),
					),
				},
			},
		})
	}
}

func TestAccRedshiftGrant_BasicSchema(t *testing.T) {
	groupNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group@tf_acc_domain.tld"), "-", "_"),
	}
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_basic"), "-", "_")

	for i, groupName := range groupNames {
		userName := userNames[i]
		config := fmt.Sprintf(`
		resource "redshift_user" "user" {
		  name = %[1]q
		}
		
		resource "redshift_group" "group" {
		  name = %[2]q
		}
		
		resource "redshift_schema" "schema" {
		  name = %[3]q
		
		  owner = redshift_user.user.name
		}
		
		resource "redshift_grant" "grant" {
		  group = redshift_group.group.name
		  schema = redshift_schema.schema.name
		
		  object_type = "schema"
		  privileges = ["create", "usage", "alter"]
		}
		
		resource "redshift_grant" "grant_user" {
		  user = redshift_user.user.name
		  schema = redshift_schema.schema.name
		  
		  object_type = "schema"
		  privileges = ["create", "usage", "alter"]
		}
		`, userName, groupName, schemaName)
		resource.Test(t, resource.TestCase{
			PreCheck:     func() { testAccPreCheck(t) },
			Providers:    testAccProviders,
			CheckDestroy: func(s *terraform.State) error { return nil },
			Steps: []resource.TestStep{
				{
					Config: config,
					Check: resource.ComposeTestCheckFunc(
						resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("gn:%s_ot:schema_%s", groupName, schemaName)),
						resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
						resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "schema"),
						resource.TestCheckResourceAttr("redshift_grant.grant", "privileges.#", "2"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "create"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "usage"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "alter"),

						resource.TestCheckResourceAttr("redshift_grant.grant_user", "id", fmt.Sprintf("un:%s_ot:schema_%s", userName, schemaName)),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "user", userName),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "object_type", "schema"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "privileges.#", "2"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "create"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "usage"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "alter"),
					),
				},
			},
		})
	}
}

func TestAccRedshiftGrant_BasicTable(t *testing.T) {
	groupNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group@tf_acc_domain.tld"), "-", "_"),
	}
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}

	for i, groupName := range groupNames {
		userName := userNames[i]
		config := fmt.Sprintf(`
		resource "redshift_group" "group" {
		  name = %[1]q
		}
		
		resource "redshift_user" "user" {
		  name = %[2]q
		  password = "TestPassword123"
		}
		
		resource "redshift_grant" "grant" {
		  group = redshift_group.group.name
		  schema = "pg_catalog"
		
		  object_type = "table"
		  objects = ["pg_user_info"]
		  privileges = ["select", "update", "insert", "delete", "drop", "references", "rule", "trigger", "truncate", "alter"]
		}
		
		resource "redshift_grant" "grant_user" {
		  user = redshift_user.user.name
		  schema = "pg_catalog"
		
		  object_type = "table"
		  objects = ["pg_user_info"]
		  privileges = ["select", "update", "insert", "delete", "drop", "references", "rule", "trigger", "truncate", "alter"]
		}
		`, groupName, userName)
		resource.Test(t, resource.TestCase{
			PreCheck:     func() { testAccPreCheck(t) },
			Providers:    testAccProviders,
			CheckDestroy: func(s *terraform.State) error { return nil },
			Steps: []resource.TestStep{
				{
					Config: config,
					Check: resource.ComposeTestCheckFunc(
						resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("gn:%s_ot:table_pg_catalog_pg_user_info", groupName)),
						resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
						resource.TestCheckResourceAttr("redshift_grant.grant", "schema", "pg_catalog"),
						resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "table"),
						resource.TestCheckResourceAttr("redshift_grant.grant", "objects.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "objects.*", "pg_user_info"),
						resource.TestCheckResourceAttr("redshift_grant.grant", "privileges.#", "8"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "select"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "update"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "insert"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "delete"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "drop"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "references"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "rule"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "trigger"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "truncate"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "alter"),

						resource.TestCheckResourceAttr("redshift_grant.grant_user", "id", fmt.Sprintf("un:%s_ot:table_pg_catalog_pg_user_info", userName)),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "user", userName),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "schema", "pg_catalog"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "object_type", "table"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "objects.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "objects.*", "pg_user_info"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "privileges.#", "8"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "select"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "update"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "insert"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "delete"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "drop"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "references"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "rule"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "trigger"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "truncate"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "alter"),
					),
				},
			},
		})
	}
}

func TestAccRedshiftGrant_BasicCallables(t *testing.T) {
	groupNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group@tf_acc_domain.tld"), "-", "_"),
	}
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}
	schema := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_basic"), "-", "_")

	for i, groupName := range groupNames {
		userName := userNames[i]
		resource.Test(t, resource.TestCase{
			PreCheck:     func() { testAccPreCheck(t) },
			Providers:    testAccProviders,
			CheckDestroy: func(s *terraform.State) error { return nil },
			Steps: []resource.TestStep{
				{
					Config: testAccRedshiftGrant_basicCallables_configUserGroup(userName, groupName, schema),
				},
				{
					PreConfig: func() {
						dbClient := testAccProvider.Meta().(*Client)
						conn, err := dbClient.Connect()
						defer dbClient.Close()
						if err != nil {
							t.Fatalf("couldn't start redshift connection: %s", err)
						}
						err = testAccRedshiftGrant_basicCallables_createSchemaAndCallables(t, conn, schema)
						if err != nil {
							t.Fatalf("couldn't setup database: %s", err)
						}
					},
					Config: testAccRedshiftGrant_basicCallables_configUserGroupWithGrants(userName, groupName, schema),
					Check: resource.ComposeTestCheckFunc(
						resource.TestCheckResourceAttr("redshift_grant.grant_fun", "id", fmt.Sprintf("gn:%s_ot:function_%s_test_call(float,float)", groupName, schema)),
						resource.TestCheckResourceAttr("redshift_grant.grant_fun", "group", groupName),
						resource.TestCheckResourceAttr("redshift_grant.grant_fun", "object_type", "function"),
						resource.TestCheckResourceAttr("redshift_grant.grant_fun", "privileges.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_fun", "privileges.*", "execute"),
						resource.TestCheckResourceAttr("redshift_grant.grant_proc", "id", fmt.Sprintf("gn:%s_ot:procedure_%s_test_call()", groupName, schema)),
						resource.TestCheckResourceAttr("redshift_grant.grant_proc", "group", groupName),
						resource.TestCheckResourceAttr("redshift_grant.grant_proc", "object_type", "procedure"),
						resource.TestCheckResourceAttr("redshift_grant.grant_proc", "privileges.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_proc", "privileges.*", "execute"),

						resource.TestCheckResourceAttr("redshift_grant.grant_user_fun", "id", fmt.Sprintf("un:%s_ot:function_%s_test_call(int,int)_test_call(float,float)", userName, schema)),
						resource.TestCheckResourceAttr("redshift_grant.grant_user_fun", "user", userName),
						resource.TestCheckResourceAttr("redshift_grant.grant_user_fun", "object_type", "function"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user_fun", "privileges.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user_fun", "privileges.*", "execute"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user_proc", "id", fmt.Sprintf("un:%s_ot:procedure_%s_test_call()", userName, schema)),
						resource.TestCheckResourceAttr("redshift_grant.grant_user_proc", "user", userName),
						resource.TestCheckResourceAttr("redshift_grant.grant_user_proc", "object_type", "procedure"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user_proc", "privileges.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user_proc", "privileges.*", "execute"),
					),
				},
				{
					Config:  testAccRedshiftGrant_basicCallables_configUserGroupWithGrants(userName, groupName, schema),
					Destroy: true,
				},
				// Creating additional dummy step as TestStep does not have PostConfig
				// property, so clean up cannot be performed in the previous one.
				{
					PreConfig: func() {
						dbClient := testAccProvider.Meta().(*Client)
						conn, err := dbClient.Connect()
						defer dbClient.Close()
						if err != nil {
							t.Errorf("couldn't cleanup resources: %s", err)
						}
						err = testAccRedshiftGrant_basicCallables_dropResources(t, conn, schema)
						if err != nil {
							t.Errorf("couldn't cleanup resources: %s", err)
						}
					},
					Config:   testAccRedshiftGrant_basicCallables_configUserGroupWithGrants(userName, groupName, schema),
					PlanOnly: true,
					Destroy:  true,
				},
			},
		})
	}
}

func TestAccRedshiftGrant_BasicLanguage(t *testing.T) {
	groupNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group@tf_acc_domain.tld"), "-", "_"),
	}
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}
	addedLanguage := "plpythonu"
	secondLanguage := "plpgsql"

	for i, groupName := range groupNames {
		userName := userNames[i]
		config := fmt.Sprintf(`
		resource "redshift_user" "user" {
		  name = %[1]q
		}
		
		resource "redshift_group" "group" {
		  name = %[2]q
		}
		
		resource "redshift_grant" "grant" {
		  group  = redshift_group.group.name
		  objects = [%[3]q, %[4]q]
		
		  object_type = "language"
		  privileges = ["usage"]
		}
		
		resource "redshift_grant" "grant_user" {
		  user = redshift_user.user.name
		  objects = [%[3]q, %[4]q]
		
		  object_type = "language"
		  privileges = ["usage"]
		}
		`, userName, groupName, addedLanguage, secondLanguage)
		resource.Test(t, resource.TestCase{
			PreCheck:     func() { testAccPreCheck(t) },
			Providers:    testAccProviders,
			CheckDestroy: func(s *terraform.State) error { return nil },
			Steps: []resource.TestStep{
				{
					Config: config,
					Check: resource.ComposeTestCheckFunc(
						resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("gn:%s_ot:language_%s_%s", groupName, addedLanguage, secondLanguage)),
						resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
						resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "language"),
						resource.TestCheckResourceAttr("redshift_grant.grant", "privileges.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "usage"),

						resource.TestCheckResourceAttr("redshift_grant.grant_user", "id", fmt.Sprintf("un:%s_ot:language_%s_%s", userName, addedLanguage, secondLanguage)),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "user", userName),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "object_type", "language"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "privileges.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "usage"),
					),
				},
			},
		})
	}
}

func TestAccRedshiftGrant_Regression_GH_Issue_24(t *testing.T) {
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_grant"), "-", "_")
	dbName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_db_grant"), "-", "_")

	for _, userName := range userNames {
		config := fmt.Sprintf(`
		resource "redshift_user" "user" {
		name = %[1]q
		}

		# Create a group named the same as user
		resource "redshift_group" "group" {
		name = %[1]q
		}

		# Create a schema and set user as owner
		resource "redshift_schema" "schema" {
		name = %[2]q

		owner = redshift_user.user.name
		}

		# The schema owner user will have all (create, usage) privileges on the schema
		# Set only 'create' privilege to a group with the same name as user. In previous versions this would trigger a permanent diff in plan.
		resource "redshift_grant" "schema" {
		group = redshift_group.group.name
		schema = redshift_schema.schema.name

		object_type = "schema"
		privileges = ["create"]
		}
		`, userName, schemaName, dbName)
		resource.Test(t, resource.TestCase{
			PreCheck:     func() { testAccPreCheck(t) },
			Providers:    testAccProviders,
			CheckDestroy: func(s *terraform.State) error { return nil },
			Steps: []resource.TestStep{
				{
					Config: config,
					Check:  resource.ComposeTestCheckFunc(),
				},
				// The 'ExpectNonEmptyPlan: false' option will fail the test if second run on the same config  will show any changes
				{
					Config:             config,
					Check:              resource.ComposeTestCheckFunc(),
					ExpectNonEmptyPlan: false,
				},
			},
		})
	}
}

func TestAccRedshiftGrant_Regression_Issue_43(t *testing.T) {
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user_grant"), "-", "_")

	config := fmt.Sprintf(`
resource "redshift_user" "user" {
  name      = %[1]q
}

resource "redshift_group" "y_schema" {
  name  = "y_schema"
  users = [redshift_user.user.name]
}

resource "redshift_group" "y" {
  name  = "y"
  users = [redshift_user.user.name]
}

resource "redshift_schema" "x" {
  name  = "x"
  owner = redshift_user.user.name
}

resource "redshift_schema" "schema_x" {
  name  = "schema_x"
  owner = redshift_user.user.name
}

resource "redshift_grant" "grants1" {
  group       = redshift_group.y_schema.name
  schema      = redshift_schema.x.name
  object_type = "schema"
  privileges  = ["USAGE"]
}

resource "redshift_grant" "grants2" {
  group       = redshift_group.y.name
  schema      = redshift_schema.schema_x.name
  object_type = "schema"
  privileges  = ["USAGE"]
}
`, userName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check:  testAccRedshiftGrant_Regression_Issue_43_compare_ids("redshift_grant.grants1", "redshift_grant.grants2"),
			},
		},
	})
}

func testAccRedshiftGrant_Regression_Issue_43_compare_ids(addr1 string, addr2 string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs1, ok := s.RootModule().Resources[addr1]
		if !ok {
			return fmt.Errorf("Not found: %s", addr1)
		}
		rs2, ok := s.RootModule().Resources[addr2]
		if !ok {
			return fmt.Errorf("Not found: %s", addr2)
		}

		if rs1.Primary.ID == rs2.Primary.ID {
			return fmt.Errorf("Resources %s and %s have the same ID: %s", addr1, addr2, rs1.Primary.ID)
		}

		return nil
	}
}

func testAccRedshiftGrant_basicCallables_configUserGroup(username, group, schema string) string {
	return fmt.Sprintf(`
resource "redshift_user" "user" {
  name = %[1]q
}

resource "redshift_group" "group" {
  name = %[2]q
}
`, username, group)
}

func testAccRedshiftGrant_basicCallables_configUserGroupWithGrants(username, group, schema string) string {
	return fmt.Sprintf(`
resource "redshift_user" "user" {
  name = %[1]q
}

resource "redshift_group" "group" {
  name = %[2]q
}

resource "redshift_grant" "grant_fun" {
	schema = %[3]q
  group  = redshift_group.group.name
  objects = ["test_call(float,float)"]

  object_type = "function"
  privileges = ["execute"]
}

resource "redshift_grant" "grant_proc" {
	schema = %[3]q
  group  = redshift_group.group.name
  objects = ["test_call()"]

  object_type = "procedure"
  privileges = ["execute"]
}

resource "redshift_grant" "grant_user_fun" {
	schema = %[3]q
  user = redshift_user.user.name
  objects = ["test_call(float,float)", "test_call(int,int)"]

  object_type = "function"
  privileges = ["execute"]
}

resource "redshift_grant" "grant_user_proc" {
	schema = %[3]q
  user = redshift_user.user.name
  objects = ["test_call()"]

  object_type = "procedure"
  privileges = ["execute"]
}
`, username, group, schema)
}

func testAccRedshiftGrant_basicCallables_createSchemaAndCallables(t *testing.T, db *DBConnection, schema string) error {
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s", pq.QuoteIdentifier(schema)))
	if err != nil {
		return fmt.Errorf("couldn't create schema: %s", err)
	}

	function := fmt.Sprintf(`
	create function %s.test_call (a float, b float)
		returns float
	stable
	as $$
		if a > b:
			return a
		return b
	$$ language plpythonu;
`, schema)

	_, err = db.Exec(function)
	if err != nil {
		return fmt.Errorf("couldn't create function: %s", err)
	}

	function2 := fmt.Sprintf(`
	create function %s.test_call (a int, b int)
		returns int
	stable
	as $$
		if a > b:
			return a
		return b
	$$ language plpythonu;
`, schema)

	_, err = db.Exec(function2)
	if err != nil {
		return fmt.Errorf("couldn't create function2: %s", err)
	}

	procedure := fmt.Sprintf(`
	CREATE PROCEDURE %s.test_call() AS $$
		BEGIN
	RAISE NOTICE 'Hello, world!';
		END
	$$ LANGUAGE plpgsql;
	`, schema)

	_, err = db.Exec(procedure)
	if err != nil {
		return fmt.Errorf("couldn't create procedure: %s", err)
	}

	return nil
}

func testAccRedshiftGrant_basicCallables_dropResources(t *testing.T, db *DBConnection, schema string) error {
	query := fmt.Sprintf("DROP SCHEMA %s CASCADE", pq.QuoteIdentifier(schema))
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("couldn't drop test schema: %s", err)
	}
	return nil
}
