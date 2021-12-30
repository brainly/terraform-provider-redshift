package redshift

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccRedshiftUser_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftUserDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftUserConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftUserExists("user_simple"),
					resource.TestCheckResourceAttr("redshift_user.simple", "name", "user_simple"),

					testAccCheckRedshiftUserExists("John-and-Jane.doe@example.com"),
					resource.TestCheckResourceAttr("redshift_user.with_email", "name", "john-and-jane.doe@example.com"),
					testAccCheckRedshiftUserCanLogin("John-and-Jane.doe@example.com", "Foobarbaz1"),

					testAccCheckRedshiftUserExists("user_defaults"),
					resource.TestCheckResourceAttr("redshift_user.user_with_defaults", "name", "user_defaults"),
					resource.TestCheckResourceAttr("redshift_user.user_with_defaults", "superuser", "false"),
					resource.TestCheckResourceAttr("redshift_user.user_with_defaults", "create_database", "false"),
					resource.TestCheckResourceAttr("redshift_user.user_with_defaults", "connection_limit", "-1"),
					resource.TestCheckResourceAttr("redshift_user.user_with_defaults", "password", ""),
					resource.TestCheckResourceAttr("redshift_user.user_with_defaults", "valid_until", "infinity"),
					resource.TestCheckResourceAttr("redshift_user.user_with_defaults", "syslog_access", "RESTRICTED"),

					testAccCheckRedshiftUserExists("user_create_database"),
					resource.TestCheckResourceAttr("redshift_user.user_with_create_database", "name", "user_create_database"),
					resource.TestCheckResourceAttr("redshift_user.user_with_create_database", "create_database", "true"),

					testAccCheckRedshiftUserExists("user_syslog"),
					resource.TestCheckResourceAttr("redshift_user.user_with_unrestricted_syslog", "name", "user_syslog"),
					resource.TestCheckResourceAttr("redshift_user.user_with_unrestricted_syslog", "syslog_access", "UNRESTRICTED"),

					testAccCheckRedshiftUserExists("user_superuser"),
					resource.TestCheckResourceAttr("redshift_user.user_superuser", "name", "user_superuser"),
					resource.TestCheckResourceAttr("redshift_user.user_superuser", "superuser", "true"),
				),
			},
		},
	})
}

func TestAccRedshiftUser_Update(t *testing.T) {

	var configCreate = `
resource "redshift_user" "update_user" {
  name = "update_user"
  password = "Foobarbaz1"
  valid_until = "2038-01-04 12:00:00+00"
}
`

	var configUpdate = `
resource "redshift_user" "update_user" {
  name = "update_user2"
  connection_limit = 5
  password = "Foobarbaz5"
  syslog_access = "UNRESTRICTED"
  create_database = true
}
`
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftUserDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftUserExists("update_user"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "name", "update_user"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "connection_limit", "-1"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "password", "Foobarbaz1"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "valid_until", "2038-01-04 12:00:00+00"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "syslog_access", "RESTRICTED"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "create_database", "false"),
				),
			},
			{
				Config: configUpdate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftUserExists("update_user2"),
					resource.TestCheckResourceAttr(
						"redshift_user.update_user", "name", "update_user2",
					),
					resource.TestCheckResourceAttr("redshift_user.update_user", "connection_limit", "5"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "password", "Foobarbaz5"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "valid_until", "infinity"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "syslog_access", "UNRESTRICTED"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "create_database", "true"),
				),
			},
			// apply the first one again to check if all parameters roll back properly
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftUserExists("update_user"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "name", "update_user"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "connection_limit", "-1"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "password", "Foobarbaz1"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "valid_until", "2038-01-04 12:00:00+00"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "syslog_access", "RESTRICTED"),
					resource.TestCheckResourceAttr("redshift_user.update_user", "create_database", "false"),
				),
			},
		},
	})
}

func TestAccRedshiftUser_UpdateToSuperuser(t *testing.T) {

	var configCreate = `
resource "redshift_user" "update_superuser" {
  name = "update_superuser"
  password = "Foobarbaz1"
}
`

	var configUpdate = `
resource "redshift_user" "update_superuser" {
  name = "update_superuser"
  password = "Foobarbaz1"
  superuser = true
}
`
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftUserDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftUserExists("update_superuser"),
					resource.TestCheckResourceAttr("redshift_user.update_superuser", "name", "update_superuser"),
					resource.TestCheckResourceAttr("redshift_user.update_superuser", "password", "Foobarbaz1"),
					resource.TestCheckResourceAttr("redshift_user.update_superuser", "syslog_access", "RESTRICTED"),
					resource.TestCheckResourceAttr("redshift_user.update_superuser", "superuser", "false"),
					//testAccCheckUserCanLogin(t, "update_superuser", "toto"),
				),
			},
			{
				Config: configUpdate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftUserExists("update_superuser"),
					resource.TestCheckResourceAttr(
						"redshift_user.update_superuser", "name", "update_superuser",
					),
					resource.TestCheckResourceAttr("redshift_user.update_superuser", "password", "Foobarbaz1"),
					resource.TestCheckResourceAttr("redshift_user.update_superuser", "syslog_access", "UNRESTRICTED"),
					resource.TestCheckResourceAttr("redshift_user.update_superuser", "superuser", "true"),
					//testAccCheckUserCanLogin(t, "update_superuser2", "titi"),
				),
			},
			// apply the first one again to test that the granted role is correctly
			// revoked and the search path has been reset to default.
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftUserExists("update_superuser"),
					resource.TestCheckResourceAttr("redshift_user.update_superuser", "name", "update_superuser"),
					resource.TestCheckResourceAttr("redshift_user.update_superuser", "password", "Foobarbaz1"),
					resource.TestCheckResourceAttr("redshift_user.update_superuser", "syslog_access", "RESTRICTED"),
					resource.TestCheckResourceAttr("redshift_user.update_superuser", "superuser", "false"),
					//testAccCheckUserCanLogin(t, "update_superuser", "toto"),
				),
			},
		},
	})
}

func TestAccRedshiftUser_SuperuserRequiresPassword(t *testing.T) {
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_superuser"), "-", "_")
	config := fmt.Sprintf(`
resource "redshift_user" "superuser" {
  name = %[1]q
  superuser = true
}
`, userName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckRedshiftUserDestroy,
		Steps: []resource.TestStep{
			{
				Config:      config,
				ExpectError: regexp.MustCompile("\"superuser\": all of `password,superuser` must be specified"),
			},
		},
	})
}

func testAccCheckRedshiftUserDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "redshift_user" {
			continue
		}

		exists, err := checkUserExists(client, rs.Primary.ID)

		if err != nil {
			return fmt.Errorf("Error checking role %s", err)
		}

		if exists {
			return fmt.Errorf("User still exists after destroy")
		}
	}

	return nil
}

func testAccCheckRedshiftUserExists(user string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkUserExists(client, user)
		if err != nil {
			return fmt.Errorf("Error checking user %s", err)
		}

		if !exists {
			return fmt.Errorf("User not found")
		}

		return nil
	}
}

func checkUserExists(client *Client, user string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}
	var _rez int
	err = db.QueryRow("SELECT 1 from pg_user_info WHERE usename=$1", strings.ToLower(user)).Scan(&_rez)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about user: %s", err)
	}

	return true, nil
}

const testAccRedshiftUserConfig = `
resource "redshift_user" "simple" {
  name = "user_simple"
}

resource "redshift_user" "with_email" {
  name = "John-and-Jane.doe@example.com"
  password = "Foobarbaz1"
}

resource "redshift_user" "user_with_defaults" {
  name = "user_defaults"
  valid_until = "infinity"
  superuser = false
  create_database = false
  connection_limit = -1
  password = ""
}

resource "redshift_user" "user_with_create_database" {
  name = "user_create_database"
  create_database = true
}

resource "redshift_user" "user_with_unrestricted_syslog" {
  name = "user_syslog"
  syslog_access = "UNRESTRICTED"
}

resource "redshift_user" "user_superuser" {
  name = "user_superuser"
  superuser = true
  password = "FooBarBaz123"
}
`

func TestPermanentUsername(t *testing.T) {
	expected := "user"
	if result := permanentUsername(expected); result != expected {
		t.Fatalf("Calling permanentUsername on a non-prefixed username should return the username. Expected %s but was %s", expected, result)
	}
	if result := permanentUsername(fmt.Sprintf("IAM:%s", expected)); result != expected {
		t.Fatalf("permanentUsername should strip \"IAM:\" prefix. Expected %s but was %s", expected, result)
	}
	if result := permanentUsername(fmt.Sprintf("IAMA:%s", expected)); result != expected {
		t.Fatalf("permanentUsername should strip \"IAMA:\" prefix. Expected %s but was %s", expected, result)
	}
}

func testAccCheckRedshiftUserCanLogin(user string, password string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		// there doesn't seem to be a good way to extract the provider configuration
		// at runtime. However we know we've configured the provider with default settings
		// so we can mimic the same behavior
		port, ok := os.LookupEnv("REDSHIFT_PORT")
		if !ok {
			port = "5439"
		}
		portNum, err := strconv.Atoi(port)
		if err != nil {
			return fmt.Errorf("Unable to check if user can login due to bad REDSHIFT_PORT: %s", err)
		}
		database, ok := os.LookupEnv("REDSHIFT_DATABASE")
		if !ok {
			database = "redshift"
		}
		sslMode, ok := os.LookupEnv("REDSHIFT_SSLMODE")
		if !ok {
			sslMode = "require"
		}
		config := &Config{
			Host:     os.Getenv("REDSHIFT_HOST"),
			Port:     portNum,
			Username: user,
			Password: password,
			Database: database,
			SSLMode:  sslMode,
			MaxConns: defaultProviderMaxOpenConnections,
		}

		client, err := config.Client()
		if err != nil {
			return fmt.Errorf("User is unable to login %s", err)
		}
		defer client.Close()
		return nil
	}
}
