package redshift

import (
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

var (
	testAccProviders map[string]*schema.Provider
	testAccProvider  *schema.Provider
)

func init() {
	testAccProvider = Provider()
	testAccProviders = map[string]*schema.Provider{
		"redshift": testAccProvider,
	}
}

func TestProvider(t *testing.T) {
	if err := Provider().InternalValidate(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestProvider_impl(t *testing.T) {
	var _ *schema.Provider = Provider()
}

func testAccPreCheck(t *testing.T) {
	var host string
	if host = os.Getenv("REDSHIFT_HOST"); host == "" {
		t.Fatal("REDSHIFT_HOST must be set for acceptance tests")
	}
	if v := os.Getenv("REDSHIFT_USER"); v == "" {
		t.Fatal("REDSHIFT_USER must be set for acceptance tests")
	}
}
