package redshift

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	datasharePrivilegeShareNameAttr = "share_name"
	datasharePrivilegeNamespaceAttr = "namespace"
	datasharePrivilegeAccountAttr   = "account"
	datasharePrivilegeShareDateAttr = "share_date"
)

func redshiftDatasharePrivilege() *schema.Resource {
	return &schema.Resource{
		Description: fmt.Sprintf("Manages consumer permissions for [data sharing](https://docs.aws.amazon.com/redshift/latest/dg/datashare-overview.html).\n"+
			"\n"+
			"When managing datashare permissions between clusters in the same account, set the `%[1]s` to the consumer's namespace guid, and omit the `%[2]s`.\n"+
			"\n"+
			"When managing data share permissions across AWS accounts, set the `%[2]s` to the consumer's AWS account ID, and omit the `%[1]s`.\n"+
			"After creating the privilege through terraform, you will also need to [authorize the cross-account datashare through the AWS console](https://docs.aws.amazon.com/redshift/latest/dg/across-account.html) before consumer clusters can access it.\n"+
			"\n"+
			"Note: Data sharing is only supported on certain instance families, such as RA3.", datasharePrivilegeNamespaceAttr, datasharePrivilegeAccountAttr),
		Exists:        RedshiftResourceExistsFunc(resourceRedshiftDatasharePrivilegeExists),
		CreateContext: RedshiftResourceFunc(resourceRedshiftDatasharePrivilegeCreate),
		ReadContext:   RedshiftResourceFunc(resourceRedshiftDatasharePrivilegeRead),
		DeleteContext: RedshiftResourceFunc(resourceRedshiftDatasharePrivilegeDelete),
		CustomizeDiff: func(_ context.Context, d *schema.ResourceDiff, _ interface{}) error {
			// Exactly one of "namespace" or "account" must be specified, however
			// terraform does not let you validate across multiple top-level attributes.
			// Per https://github.com/hashicorp/terraform-plugin-sdk/issues/233, the conventional
			// workaround is to use CustomizeDiff.
			_, consumerNamespaceSet := d.GetOk(datasharePrivilegeNamespaceAttr)
			_, consumerAccountSet := d.GetOk(datasharePrivilegeAccountAttr)
			if (consumerNamespaceSet && !consumerAccountSet) || (consumerAccountSet && !consumerNamespaceSet) {
				return nil
			}
			return fmt.Errorf("Exactly one of %s or %s must be set", datasharePrivilegeNamespaceAttr, datasharePrivilegeAccountAttr)
		},
		Schema: map[string]*schema.Schema{
			datasharePrivilegeShareNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Name of the datashare",
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			datasharePrivilegeNamespaceAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "Namespace (guid) of the consumer cluster, for sharing data within the same account. Either this or `account` must be specified.",
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
				ConflictsWith: []string{
					datasharePrivilegeAccountAttr,
				},
				ValidateFunc: validation.StringMatch(uuidRegex, "Consumer namespace must be a guid"),
			},
			datasharePrivilegeAccountAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "AWS account ID where the consumer cluster is located, for sharing data across accounts. Either this or `namespace` must be specified.",
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
				ConflictsWith: []string{
					datasharePrivilegeNamespaceAttr,
				},
				ValidateFunc: validation.StringMatch(awsAccountIdRegexp, "AWS account id must be a 12-digit number"),
			},
			datasharePrivilegeShareDateAttr: {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "When the datashare permission was granted",
			},
		},
	}
}

func generateDatasharePrivilegesID(d *schema.ResourceData) string {
	shareName := d.Get(datasharePrivilegeShareNameAttr).(string)
	consumerNamespaceRaw, useNamespace := d.GetOk(datasharePrivilegeNamespaceAttr)
	consumerAccountRaw, useAccount := d.GetOk(datasharePrivilegeAccountAttr)
	source := []string{shareName}
	if useNamespace {
		source = append(source, consumerNamespaceRaw.(string))
	} else if useAccount {
		source = append(source, consumerAccountRaw.(string))
	}

	return strings.Join(source, ".")
}

func resourceRedshiftDatasharePrivilegeExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	shareName := d.Get(datasharePrivilegeShareNameAttr).(string)
	consumerNamespaceRaw, useNamespace := d.GetOk(datasharePrivilegeNamespaceAttr)
	if useNamespace {
		return resourceRedshiftDatasharePrivilegeNamespaceExists(db, shareName, consumerNamespaceRaw.(string))
	}
	consumerAccountRaw, useAccount := d.GetOk(datasharePrivilegeAccountAttr)
	if useAccount {
		return resourceRedshiftDatasharePrivilegeAccountExists(db, shareName, consumerAccountRaw.(string))
	}
	return false, fmt.Errorf("Either %s or %s is required", datasharePrivilegeNamespaceAttr, datasharePrivilegeAccountAttr)
}

func resourceRedshiftDatasharePrivilegeNamespaceExists(db *DBConnection, shareName string, consumerNamespace string) (bool, error) {
	var shareDate string
	query := "SELECT share_date FROM svv_datashare_consumers WHERE share_name = $1 AND consumer_namespace = $2"
	log.Printf("[DEBUG] %s\n", query)
	err := db.QueryRow(query, shareName, consumerNamespace).Scan(&shareDate)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func resourceRedshiftDatasharePrivilegeAccountExists(db *DBConnection, shareName string, consumerAccount string) (bool, error) {
	var shareDate string
	query := "SELECT share_date FROM svv_datashare_consumers WHERE share_name = $1 AND consumer_account = $2"
	log.Printf("[DEBUG] %s\n", query)
	err := db.QueryRow(query, shareName, consumerAccount).Scan(&shareDate)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func resourceRedshiftDatasharePrivilegeCreate(db *DBConnection, d *schema.ResourceData) error {
	shareName := d.Get(datasharePrivilegeShareNameAttr).(string)
	consumerNamespaceRaw, consumerNamespaceSet := d.GetOk(datasharePrivilegeNamespaceAttr)
	consumerAccountRaw, consumerAccountSet := d.GetOk(datasharePrivilegeAccountAttr)
	query := fmt.Sprintf("GRANT USAGE ON DATASHARE %s TO ", pq.QuoteIdentifier(shareName))
	if consumerNamespaceSet {
		query = fmt.Sprintf("%s NAMESPACE '%s'", query, pqQuoteLiteral(consumerNamespaceRaw.(string)))
	} else if consumerAccountSet {
		query = fmt.Sprintf("%s ACCOUNT '%s'", query, pqQuoteLiteral(consumerAccountRaw.(string)))
	} else {
		return fmt.Errorf("Either %s or %s is required", datasharePrivilegeNamespaceAttr, datasharePrivilegeAccountAttr)
	}
	log.Printf("[DEBUG] %s\n", query)
	if _, err := db.Exec(query); err != nil {
		return err
	}

	d.SetId(generateDatasharePrivilegesID(d))

	return resourceRedshiftDatasharePrivilegeRead(db, d)
}

func resourceRedshiftDatasharePrivilegeRead(db *DBConnection, d *schema.ResourceData) error {
	shareName := d.Get(datasharePrivilegeShareNameAttr).(string)
	consumerNamespaceRaw, consumerNamespaceSet := d.GetOk(datasharePrivilegeNamespaceAttr)
	consumerAccountRaw, consumerAccountSet := d.GetOk(datasharePrivilegeAccountAttr)
	if consumerNamespaceSet {
		return resourceRedshiftDatasharePrivilegeNamespaceRead(db, shareName, consumerNamespaceRaw.(string), d)
	} else if consumerAccountSet {
		return resourceRedshiftDatasharePrivilegeAccountRead(db, shareName, consumerAccountRaw.(string), d)
	}

	return fmt.Errorf("Either %s or %s is required", datasharePrivilegeNamespaceAttr, datasharePrivilegeAccountAttr)
}

func resourceRedshiftDatasharePrivilegeNamespaceRead(db *DBConnection, shareName string, consumerNamespace string, d *schema.ResourceData) error {
	var shareDate string
	query := `SELECT
  REPLACE(TO_CHAR(share_date, 'YYYY-MM-DD HH24:MI:SS'), ' ', 'T') || 'Z'
FROM
  svv_datashare_consumers
WHERE
  share_name = $1
AND
  consumer_namespace = $2`

	log.Printf("[DEBUG] %s\n", query)
	err := db.QueryRow(query, shareName, consumerNamespace).Scan(&shareDate)
	if err != nil {
		return err
	}

	d.Set(datasharePrivilegeShareDateAttr, shareDate)

	return nil
}

func resourceRedshiftDatasharePrivilegeAccountRead(db *DBConnection, shareName string, consumerAccount string, d *schema.ResourceData) error {
	var shareDate string
	query := `SELECT
  REPLACE(TO_CHAR(share_date, 'YYYY-MM-DD HH24:MI:SS'), ' ', 'T') || 'Z'
FROM
  svv_datashare_consumers
WHERE
  share_name = $1
AND
  consumer_account = $2`

	log.Printf("[DEBUG] %s\n", query)
	err := db.QueryRow(query, shareName, consumerAccount).Scan(&shareDate)
	if err != nil {
		return err
	}

	d.Set(datasharePrivilegeShareDateAttr, shareDate)

	return nil
}

func resourceRedshiftDatasharePrivilegeDelete(db *DBConnection, d *schema.ResourceData) error {
	shareName := d.Get(datasharePrivilegeShareNameAttr).(string)
	consumerNamespaceRaw, consumerNamespaceSet := d.GetOk(datasharePrivilegeNamespaceAttr)
	consumerAccountRaw, consumerAccountSet := d.GetOk(datasharePrivilegeAccountAttr)
	query := fmt.Sprintf("REVOKE USAGE ON DATASHARE %s FROM", pq.QuoteIdentifier(shareName))
	if consumerNamespaceSet {
		query = fmt.Sprintf("%s NAMESPACE '%s'", query, pqQuoteLiteral(consumerNamespaceRaw.(string)))
	} else if consumerAccountSet {
		query = fmt.Sprintf("%s ACCOUNT '%s'", query, consumerAccountRaw.(string))
	}
	log.Printf("[DEBUG] %s\n", query)

	_, err := db.Exec(query)
	return err
}
