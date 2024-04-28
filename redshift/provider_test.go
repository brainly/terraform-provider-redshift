package redshift

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

var (
	testAccProviders map[string]func() (*schema.Provider, error)
	testAccProvider  *schema.Provider
)

func init() {
	testAccProvider = Provider()
	testAccProviders = map[string]func() (*schema.Provider, error){
		"redshift": func() (*schema.Provider, error) { return testAccProvider, nil },
	}
}

func TestProvider(t *testing.T) {
	if err := Provider().InternalValidate(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestProvider_impl(t *testing.T) {
	var _ = Provider()
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
	clusterIdentifier := getEnvOrSkip("REDSHIFT_TEMPORARY_CREDENTIALS_CLUSTER_IDENTIFIER", t)

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
	if arn, ok := os.LookupEnv("REDSHIFT_TEMPORARY_CREDENTIALS_ASSUME_ROLE_ARN"); ok {
		config["temporary_credentials"].([]interface{})[0].(map[string]interface{})["assume_role"] = []interface{}{
			map[string]interface{}{
				"arn": arn,
			},
		}
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
	assume_role_arn := os.Getenv("REDSHIFT_TEMPORARY_CREDENTIALS_ASSUME_ROLE_ARN")
	defer os.Setenv("REDSHIFT_TEMPORARY_CREDENTIALS_ASSUME_ROLE_ARN", assume_role_arn)
	os.Unsetenv("REDSHIFT_TEMPORARY_CREDENTIALS_ASSUME_ROLE_ARN")
	prepareRedshiftTemporaryCredentialsTestCases(t, provider)
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

func TestAccRedshiftTemporaryCredentialsAssumeRole(t *testing.T) {
	_ = getEnvOrSkip("REDSHIFT_TEMPORARY_CREDENTIALS_ASSUME_ROLE_ARN", t)
	provider := Provider()
	prepareRedshiftTemporaryCredentialsTestCases(t, provider)
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

func prepareRedshiftTemporaryCredentialsTestCases(t *testing.T, provider *schema.Provider) {
	redshift_password := os.Getenv("REDSHIFT_PASSWORD")
	defer os.Setenv("REDSHIFT_PASSWORD", redshift_password)
	os.Unsetenv("REDSHIFT_PASSWORD")
	rawUsername := os.Getenv("REDSHIFT_USER")
	defer os.Setenv("REDSHIFT_USER", rawUsername)
	username := strings.ToLower(permanentUsername(rawUsername))
	os.Setenv("REDSHIFT_USER", username)
	initTemporaryCredentialsProvider(t, provider)
}
