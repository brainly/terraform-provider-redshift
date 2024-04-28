package redshift

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const databaseNameAttr = "name"
const databaseOwnerAttr = "owner"
const databaseConnLimitAttr = "connection_limit"
const databaseDatashareSourceAttr = "datashare_source"
const databaseDatashareSourceShareNameAttr = "share_name"
const databaseDatashareSourceNamespaceAttr = "namespace"
const databaseDatashareSourceAccountAttr = "account_id"

func redshiftDatabase() *schema.Resource {
	return &schema.Resource{
		Description:   `Defines a local database.`,
		Exists:        RedshiftResourceExistsFunc(resourceRedshiftDatabaseExists),
		CreateContext: RedshiftResourceFunc(resourceRedshiftDatabaseCreate),
		ReadContext:   RedshiftResourceFunc(resourceRedshiftDatabaseRead),
		UpdateContext: RedshiftResourceFunc(resourceRedshiftDatabaseUpdate),
		DeleteContext: RedshiftResourceFunc(resourceRedshiftDatabaseDelete),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		CustomizeDiff: forceNewIfListSizeChanged(databaseDatashareSourceAttr),
		Schema: map[string]*schema.Schema{
			databaseNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of the database",
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			databaseOwnerAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				Description: "Owner of the database, usually the user who created it",
			},
			databaseConnLimitAttr: {
				Type:         schema.TypeInt,
				Optional:     true,
				Description:  "The maximum number of concurrent connections that can be made to this database. A value of -1 means no limit.",
				Default:      -1,
				ValidateFunc: validation.IntAtLeast(-1),
			},
			databaseDatashareSourceAttr: {
				Type:        schema.TypeList,
				Optional:    true,
				MaxItems:    1,
				Description: "Configuration for creating a database from a redshift datashare.",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						databaseDatashareSourceShareNameAttr: {
							Type:        schema.TypeString,
							Required:    true,
							ForceNew:    true,
							Description: "The name of the datashare on the producer cluster",
							StateFunc: func(val interface{}) string {
								return strings.ToLower(val.(string))
							},
						},
						databaseDatashareSourceNamespaceAttr: {
							Type:        schema.TypeString,
							Required:    true,
							ForceNew:    true,
							Description: "The namespace (guid) of the producer cluster",
							StateFunc: func(val interface{}) string {
								return strings.ToLower(val.(string))
							},
						},
						databaseDatashareSourceAccountAttr: {
							Type:         schema.TypeString,
							Optional:     true,
							ForceNew:     true,
							Computed:     true,
							Description:  "The AWS account ID of the producer cluster.",
							ValidateFunc: validation.StringMatch(awsAccountIdRegexp, "AWS account id must be a 12-digit number"),
						},
					},
				},
			},
		},
	}
}

func resourceRedshiftDatabaseExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	var name string
	query := "SELECT datname FROM pg_database WHERE oid = $1"
	log.Printf("[DEBUG] check if database exists: %s\n", query)
	err := db.QueryRow(query, d.Id()).Scan(&name)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func resourceRedshiftDatabaseCreate(db *DBConnection, d *schema.ResourceData) error {
	if _, isDataShare := d.GetOk(fmt.Sprintf("%s.0.%s", databaseDatashareSourceAttr, databaseDatashareSourceShareNameAttr)); isDataShare {
		return resourceRedshiftDatabaseCreateFromDatashare(db, d)
	}
	return resourceRedshiftDatabaseCreateInternal(db, d)
}

func resourceRedshiftDatabaseCreateFromDatashare(db *DBConnection, d *schema.ResourceData) error {
	dbName := d.Get(databaseNameAttr).(string)
	shareName := d.Get(fmt.Sprintf("%s.0.%s", databaseDatashareSourceAttr, databaseDatashareSourceShareNameAttr)).(string)
	query := fmt.Sprintf("CREATE DATABASE %s FROM DATASHARE %s OF", pq.QuoteIdentifier(dbName), pq.QuoteIdentifier(shareName))
	if sourceAccount, ok := d.GetOk(fmt.Sprintf("%s.0.%s", databaseDatashareSourceAttr, databaseDatashareSourceAccountAttr)); ok {
		query = fmt.Sprintf("%s ACCOUNT '%s'", query, pqQuoteLiteral(sourceAccount.(string)))
	}
	namespace := d.Get(fmt.Sprintf("%s.0.%s", databaseDatashareSourceAttr, databaseDatashareSourceNamespaceAttr))
	query = fmt.Sprintf("%s NAMESPACE '%s'", query, pqQuoteLiteral(namespace.(string)))

	if _, err := db.Exec(query); err != nil {
		return err
	}

	// eagerly get the resource ID in case the below statements fail for some reason
	var oid string
	query = "SELECT oid FROM pg_database WHERE datname = $1"
	log.Printf("[DEBUG] get oid from database: %s\n", query)
	if err := db.QueryRow(query, strings.ToLower(dbName)).Scan(&oid); err != nil {
		return err
	}
	d.SetId(oid)

	// CREATE DATABASE isn't allowed to run inside a transaction, however ALTER DATABASE
	// can be
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	// CREATE DATABASE FROM DATASHARE... doesn't allow you to specify an owner in the create statement,
	// so we need to set the owner after creation using ALTER DATABASE...
	owner, ownerIsSet := d.GetOk(databaseOwnerAttr)
	if ownerIsSet {
		if _, err = tx.Exec(fmt.Sprintf("ALTER DATABASE %s OWNER TO %s", pq.QuoteIdentifier(dbName), pq.QuoteIdentifier(owner.(string)))); err != nil {
			return err
		}
	}

	// CREATE DATABASE FROM DATASHARE... doesn't allow you to specify the connection limit in the create statement,
	// so we need to set the owner after creation using ALTER DATABASE...
	connLimit, connLimitIsSet := d.GetOk(databaseConnLimitAttr)
	if connLimitIsSet {
		if _, err = tx.Exec(fmt.Sprintf("ALTER DATABASE %s CONNECTION LIMIT %d", pq.QuoteIdentifier(dbName), connLimit.(int))); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}

	return resourceRedshiftDatabaseRead(db, d)
}

func resourceRedshiftDatabaseCreateInternal(db *DBConnection, d *schema.ResourceData) error {
	dbName := d.Get(databaseNameAttr).(string)
	query := fmt.Sprintf("CREATE DATABASE %s", pq.QuoteIdentifier(dbName))

	if v, ok := d.GetOk(databaseOwnerAttr); ok {
		query = fmt.Sprintf("%s OWNER %s", query, pq.QuoteIdentifier(v.(string)))
	}
	if v, ok := d.GetOk(databaseConnLimitAttr); ok {
		query = fmt.Sprintf("%s CONNECTION LIMIT %d", query, v.(int))
	}
	log.Printf("[DEBUG] create database %s: %s\n", dbName, query)
	if _, err := db.Exec(query); err != nil {
		return err
	}

	var oid string
	query = "SELECT oid FROM pg_database WHERE datname = $1"
	log.Printf("[DEBUG] get oid from database: %s\n", query)
	if err := db.QueryRow(query, strings.ToLower(dbName)).Scan(&oid); err != nil {
		return err
	}

	d.SetId(oid)

	return resourceRedshiftDatabaseRead(db, d)
}

func resourceRedshiftDatabaseRead(db *DBConnection, d *schema.ResourceData) error {
	var name, owner, connLimit, databaseType, shareName, producerAccount, producerNamespace string

	query := `SELECT
  TRIM(svv_redshift_databases.database_name),
  TRIM(pg_user_info.usename),
  COALESCE(pg_database_info.datconnlimit::text, 'UNLIMITED'),
	svv_redshift_databases.database_type,
  TRIM(COALESCE(svv_datashares.share_name, '')),
  TRIM(COALESCE(svv_datashares.producer_account, '')),
  TRIM(COALESCE(svv_datashares.producer_namespace, ''))
FROM
  svv_redshift_databases
LEFT JOIN pg_database_info
  ON svv_redshift_databases.database_name=pg_database_info.datname
LEFT JOIN pg_user_info
  ON pg_user_info.usesysid = svv_redshift_databases.database_owner
LEFT JOIN svv_datashares
	ON (svv_redshift_databases.database_name = svv_datashares.consumer_database AND svv_redshift_databases.database_type = 'shared' AND svv_datashares.share_type = 'INBOUND')
WHERE pg_database_info.datid = $1
`
	log.Printf("[DEBUG] read database: %s\n", query)
	err := db.QueryRow(query, d.Id()).Scan(&name, &owner, &connLimit, &databaseType, &shareName, &producerAccount, &producerNamespace)

	if err != nil {
		return err
	}

	connLimitNumber := -1
	if connLimit != "UNLIMITED" {
		if connLimitNumber, err = strconv.Atoi(connLimit); err != nil {
			return err
		}
	}

	d.Set(databaseNameAttr, name)
	d.Set(databaseOwnerAttr, owner)
	d.Set(databaseConnLimitAttr, connLimitNumber)

	dataShareConfiguration := make([]map[string]interface{}, 0, 1)
	if databaseType == "shared" {
		config := make(map[string]interface{})
		config[databaseDatashareSourceShareNameAttr] = &shareName
		config[databaseDatashareSourceAccountAttr] = &producerAccount
		config[databaseDatashareSourceNamespaceAttr] = &producerNamespace
		dataShareConfiguration = append(dataShareConfiguration, config)
	}
	d.Set(databaseDatashareSourceAttr, dataShareConfiguration)

	return nil
}

func resourceRedshiftDatabaseUpdate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := setDatabaseName(tx, d); err != nil {
		return err
	}

	if err := setDatabaseOwner(tx, d); err != nil {
		return err
	}

	if err := setDatabaseConnLimit(tx, d); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftDatabaseRead(db, d)
}

func setDatabaseName(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(databaseNameAttr) {
		return nil
	}

	oldRaw, newRaw := d.GetChange(databaseNameAttr)
	oldValue := oldRaw.(string)
	newValue := newRaw.(string)

	if newValue == "" {
		return fmt.Errorf("Error setting database name to an empty string")
	}

	query := fmt.Sprintf("ALTER DATABASE %s RENAME TO %s", pq.QuoteIdentifier(oldValue), pq.QuoteIdentifier(newValue))
	log.Printf("[DEBUG] renaming database %s to %s: %s\n", oldValue, newValue, query)
	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("Error updating database NAME: %w", err)
	}

	return nil
}

func setDatabaseOwner(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(databaseOwnerAttr) {
		return nil
	}

	databaseName := d.Get(databaseNameAttr).(string)
	databaseOwner := d.Get(databaseOwnerAttr).(string)

	query := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s", pq.QuoteIdentifier(databaseName), pq.QuoteIdentifier(databaseOwner))
	log.Printf("[DEBUG] changing database owner: %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func setDatabaseConnLimit(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(databaseConnLimitAttr) {
		return nil
	}

	databaseName := d.Get(databaseNameAttr).(string)
	connLimit := d.Get(databaseConnLimitAttr).(int)
	query := fmt.Sprintf("ALTER DATABASE %s CONNECTION LIMIT %d", pq.QuoteIdentifier(databaseName), connLimit)
	log.Printf("[DEBUG] changing database connection limit: %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatabaseDelete(db *DBConnection, d *schema.ResourceData) error {
	databaseName := d.Get(databaseNameAttr).(string)

	query := fmt.Sprintf("DROP DATABASE %s", pqQuoteLiteral(databaseName))
	log.Printf("[DEBUG] dropping database %s: %s\n", databaseName, query)
	_, err := db.Exec(query)
	return err
}
