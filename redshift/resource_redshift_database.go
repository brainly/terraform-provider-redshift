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

func redshiftDatabase() *schema.Resource {
	return &schema.Resource{
		Description: `Defines a local database.`,
		Exists:      RedshiftResourceExistsFunc(resourceRedshiftDatabaseExists),
		Create:      RedshiftResourceFunc(resourceRedshiftDatabaseCreate),
		Read:        RedshiftResourceFunc(resourceRedshiftDatabaseRead),
		Update:      RedshiftResourceFunc(resourceRedshiftDatabaseUpdate),
		Delete:      RedshiftResourceFunc(resourceRedshiftDatabaseDelete),
		Importer: &schema.ResourceImporter{
			State: RedshiftImportFunc(resourceRedshiftDatabaseImport),
		},
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
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			databaseConnLimitAttr: {
				Type:         schema.TypeInt,
				Optional:     true,
				Description:  "The maximum number of concurrent connections that can be made to this database. A value of -1 means no limit.",
				Default:      -1,
				ValidateFunc: validation.IntAtLeast(-1),
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
	var name, owner, connLimit string

	query := `SELECT
  trim(svv_redshift_databases.database_name),
  trim(pg_user_info.usename),
  COALESCE(pg_database_info.datconnlimit::text, 'UNLIMITED')
FROM
  svv_redshift_databases
LEFT JOIN pg_database_info
  ON svv_redshift_databases.database_name=pg_database_info.datname
LEFT JOIN pg_user_info
  ON pg_user_info.usesysid = svv_redshift_databases.database_owner
WHERE svv_redshift_databases.database_type = 'local'
AND pg_database_info.datid = $1
`
	log.Printf("[DEBUG] read database: %s\n", query)
	err := db.QueryRow(query, d.Id()).Scan(&name, &owner, &connLimit)

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

func resourceRedshiftDatabaseImport(db *DBConnection, d *schema.ResourceData) ([]*schema.ResourceData, error) {
	var databaseType string

	query := `SELECT
  svv_redshift_databases.database_type
FROM
  svv_redshift_databases
LEFT JOIN pg_database_info
  ON svv_redshift_databases.database_name=pg_database_info.datname
WHERE pg_database_info.datid = $1
`
	log.Printf("[DEBUG] read database for import: %s\n", query)
	err := db.QueryRow(query, d.Id()).Scan(&databaseType)
	switch {
	case err == sql.ErrNoRows:
		return nil, fmt.Errorf("No database found with oid %s", d.Id())
	case err != nil:
		return nil, err
	case databaseType != "local":
		return nil, fmt.Errorf("redshift_database resource is only for 'local' databases. Database with oid %s has type '%s'", d.Id(), databaseType)
	}

	err = resourceRedshiftDatabaseRead(db, d)
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
