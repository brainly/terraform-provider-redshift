package redshift

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
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

func initTemporaryCredentialsProvider(t *testing.T, provider *schema.Provider) {
	var clusterIdentifier string
	if clusterIdentifier = os.Getenv("REDSHIFT_CLUSTER_IDENTIFIER"); clusterIdentifier == "" {
		t.Skip("REDSHIFT_CLUSTER_IDENTIFIER must be set for acceptance tests")
	}

	sdkClient, err := stsClient(t)
	if err != nil {
		t.Skip(fmt.Sprintf("Unable to load STS client due to: %s", err))
	}

	response, err := sdkClient.GetCallerIdentity(context.TODO(), nil)
	if err != nil {
		t.Skip(fmt.Sprintf("Unable to get current STS identity due to: %s", err))
	}
	if response == nil {
		t.Skip("Unable to get current STS identity. Empty response.")
	}

	config := map[string]interface{}{
		"temporary_credentials": []interface{}{
			map[string]interface{}{
				"cluster_identifier": clusterIdentifier,
			},
		},
	}
	diagnostics := provider.Configure(context.Background(), terraform.NewResourceConfigRaw(config))
	if diagnostics != nil {
		if diagnostics.HasError() {
			t.Fatalf("Failed to configure temporary credentials provider: %v", diagnostics)
		}
	}
}

func stsClient(t *testing.T) (*sts.Client, error) {
	config, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	return sts.NewFromConfig(config), nil
}

func TestAccRedshiftTemporaryCredentials(t *testing.T) {
	provider := Provider()
	redshift_password := os.Getenv("REDSHIFT_PASSWORD")
	defer os.Setenv("REDSHIFT_PASSWORD", redshift_password)
	os.Unsetenv("REDSHIFT_PASSWORD")
	initTemporaryCredentialsProvider(t, provider)
	client, ok := provider.Meta().(*Client)
	if !ok {
		t.Fatal("Unable to initialize client")
	}
	db, err := client.Connect()
	if err != nil {
		t.Fatalf("Unable to connect to database: %s", err)
	}
	defer db.Close()
}
