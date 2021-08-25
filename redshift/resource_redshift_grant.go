package redshift

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	grantGroupAttr      = "group"
	grantSchemaAttr     = "schema"
	grantObjectTypeAttr = "object_type"
	grantObjectsAttr    = "objects"
	grantPrivilegesAttr = "privileges"
)

var grantAllowedObjectTypes = []string{
	"table",
	"schema",
	"database",
}

var grantObjectTypesCodes = map[string]string{
	"table": "r",
}

func redshiftGrant() *schema.Resource {
	return &schema.Resource{
		Description: `
Defines access privileges for user group. Privileges include access options such as being able to read data in tables and views, write data, create tables, and drop tables. Use this command to give specific privileges for a table, database, schema, function, procedure, language, or column.
`,
		Read: RedshiftResourceFunc(resourceRedshiftGrantRead),
		Create: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftGrantCreate),
		),
		Delete: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftGrantDelete),
		),

		// Since we revoke all when creating, we can use create as update
		Update: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftGrantCreate),
		),

		Schema: map[string]*schema.Schema{
			grantGroupAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the group to grant privileges on.",
			},
			grantSchemaAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "The database schema to grant privileges on for this group.",
			},
			grantObjectTypeAttr: {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringInSlice(grantAllowedObjectTypes, false),
				Description:  "The Redshift object type to grant privileges on (one of: " + strings.Join(grantAllowedObjectTypes, ", ") + ").",
			},
			grantObjectsAttr: {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
					StateFunc: func(val interface{}) string {
						return strings.ToLower(val.(string))
					},
				},
				Set:         schema.HashString,
				Description: "The objects upon which to grant the privileges. An empty list (the default) means to grant permissions on all objects of the specified type. Only has effect if `object_type` is set to `table`.",
			},
			grantPrivilegesAttr: {
				Type:     schema.TypeSet,
				Required: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
					StateFunc: func(val interface{}) string {
						return strings.ToLower(val.(string))
					},
				},
				Set:         schema.HashString,
				Description: "The list of privileges to apply as default privileges. See [GRANT command documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html) to see what privileges are available to which object type. An empty list could be provided to revoke all privileges for this group",
			},
		},
	}
}

func resourceRedshiftGrantCreate(db *DBConnection, d *schema.ResourceData) error {
	objectType := d.Get(grantObjectTypeAttr).(string)
	schemaName := d.Get(grantSchemaAttr).(string)
	objects := d.Get(grantObjectsAttr).(*schema.Set).List()

	privileges := []string{}
	for _, p := range d.Get(grantPrivilegesAttr).(*schema.Set).List() {
		privileges = append(privileges, p.(string))
	}

	// validate parameters
	if objectType == "table" && schemaName == "" {
		return fmt.Errorf("parameter `%s` is required for objects of type table", grantSchemaAttr)
	}

	if (objectType == "database" || objectType == "schema") && len(objects) > 0 {
		return fmt.Errorf("cannot specify `%s` when `%s` is `database` or `schema`", grantObjectsAttr, grantObjectTypeAttr)
	}

	if !validatePrivileges(privileges, objectType) {
		return fmt.Errorf("Invalid privileges list %v for object of type %s", privileges, objectType)
	}

	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := revokeGroupGrants(tx, db.client.databaseName, d); err != nil {
		return err
	}

	if err := createGroupGrants(tx, db.client.databaseName, d); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	d.SetId(generateGrantID(d))

	return resourceRedshiftGrantReadImpl(db, d)
}

func resourceRedshiftGrantDelete(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := revokeGroupGrants(tx, db.client.databaseName, d); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}

func resourceRedshiftGrantRead(db *DBConnection, d *schema.ResourceData) error {
	return resourceRedshiftGrantReadImpl(db, d)
}

func resourceRedshiftGrantReadImpl(db *DBConnection, d *schema.ResourceData) error {
	objectType := d.Get(grantObjectTypeAttr).(string)

	switch objectType {
	case "database":
		return readGroupDatabaseGrants(db, d)
	default:
		return fmt.Errorf("Unsupported %s %s", grantObjectTypeAttr, objectType)
	}
}

func readGroupDatabaseGrants(db *DBConnection, d *schema.ResourceData) error {
	groupName := d.Get(grantGroupAttr).(string)
	var databaseCreate, databaseTemp bool

	query := `
  SELECT
    decode(charindex('C',split_part(split_part(array_to_string(db.datacl, '|'),gr.groname,2 ) ,'/',1)), 0,0,1) as create,
    decode(charindex('T',split_part(split_part(array_to_string(db.datacl, '|'),gr.groname,2 ) ,'/',1)), 0,0,1) as temporary
  FROM pg_database db, pg_group gr
  WHERE
    db.datname=$1 
    AND gr.groname=$2
`

	if err := db.QueryRow(query, db.client.databaseName, groupName).Scan(&databaseCreate, &databaseTemp); err != nil {
		return err
	}

	privileges := []string{}
	appendIfTrue(databaseCreate, "create", &privileges)
	appendIfTrue(databaseTemp, "temporary", &privileges)

	log.Printf("[DEBUG] Collected database '%s' privileges for group %s: %v", db.client.databaseName, groupName, privileges)

	d.Set(grantPrivilegesAttr, privileges)

	return nil
}

func readGroupGrantsForTables(tx *sql.Tx, groupName, schemaName string, tablesNames []string) ([]string, error) {
	var tables, tableSelect, tableUpdate, tableInsert, tableDelete, tableReferences int
	query := `
  SELECT
    nvl(count(cl.relname), 0) tables,
    nvl(sum(decode(charindex('r',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)), 0,0,1)), 0) as select,
    nvl(sum(decode(charindex('w',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)), 0,0,1)), 0) as update,
    nvl(sum(decode(charindex('a',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)), 0,0,1)), 0) as insert,
    nvl(sum(decode(charindex('d',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)), 0,0,1)), 0) as delete,
    nvl(sum(decode(charindex('x',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)), 0,0,1)), 0) as references
  FROM pg_class cl
  JOIN pg_group gr ON array_to_string(cl.relacl, '|') LIKE '%group '||gr.groname||'=%'
  JOIN pg_namespace nsp ON nsp.oid = cl.relnamespace
  WHERE
    cl.relkind = 'r'
    AND gr.groname=$1
    AND nsp.nspname=$2
`

	var err error = nil
	if len(tablesNames) > 0 {
		query = fmt.Sprintf("%s AND cl.relname = ANY($3)", query)
		err = tx.QueryRow(query, groupName, schemaName, pq.Array(tablesNames)).Scan(tables, tableSelect, tableUpdate, tableInsert, tableDelete, tableReferences)
	} else {
		err = tx.QueryRow(query, groupName, schemaName).Scan(tableSelect, tableUpdate, tableInsert, tableDelete, tableReferences)
	}

	if err != nil {
		return []string{}, fmt.Errorf("failed to collect group privileges: %w", err)
	}

	privileges := []string{}
	expectedPrivileges := len(tablesNames)
	appendIfTrue(tableSelect == expectedPrivileges, "select", &privileges)
	appendIfTrue(tableUpdate == expectedPrivileges, "update", &privileges)
	appendIfTrue(tableInsert == expectedPrivileges, "insert", &privileges)
	appendIfTrue(tableDelete == expectedPrivileges, "delete", &privileges)
	appendIfTrue(tableReferences == expectedPrivileges, "references", &privileges)

	log.Printf("[DEBUG] Collected privileges for group  %s: %v\n", groupName, privileges)

	return privileges, nil
}

func revokeGroupGrants(tx *sql.Tx, databaseName string, d *schema.ResourceData) error {
	query := createGroupRevokeQuery(d, databaseName)
	_, err := tx.Exec(query)
	return err
}

func createGroupGrants(tx *sql.Tx, databaseName string, d *schema.ResourceData) error {
	if d.Get(grantPrivilegesAttr).(*schema.Set).Len() == 0 {
		log.Printf("[DEBUG] no privileges to grant for group %s", d.Get(grantGroupAttr).(string))
		return nil
	}

	query := createGroupGrantQuery(d, databaseName)
	_, err := tx.Exec(query)
	return err
}

func createGroupRevokeQuery(d *schema.ResourceData, databaseName string) string {
	var query string

	switch strings.ToUpper(d.Get(grantObjectTypeAttr).(string)) {
	case "DATABASE":
		query = fmt.Sprintf(
			"REVOKE ALL PRIVILEGES ON DATABASE %s FROM GROUP %s",
			pq.QuoteIdentifier(databaseName),
			pq.QuoteIdentifier(d.Get(grantGroupAttr).(string)),
		)
	case "SCHEMA":
		query = fmt.Sprintf(
			"REVOKE ALL PRIVILEGES ON SCHEMA %s FROM GROUP %s",
			pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
			pq.QuoteIdentifier(d.Get(grantGroupAttr).(string)),
		)
	case "TABLE":
		objects := d.Get(grantObjectsAttr).(*schema.Set)
		if objects.Len() > 0 {
			query = fmt.Sprintf(
				"REVOKE ALL PRIVILEGES ON %s %s FROM GROUP %s",
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				setToPgIdentList(objects, d.Get(grantSchemaAttr).(string)),
				pq.QuoteIdentifier(d.Get(grantGroupAttr).(string)),
			)
		} else {
			query = fmt.Sprintf(
				"REVOKE ALL PRIVILEGES ON ALL %sS IN SCHEMA %s FROM GROUP %s",
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
				pq.QuoteIdentifier(d.Get(grantGroupAttr).(string)),
			)
		}
	}

	return query
}

func createGroupGrantQuery(d *schema.ResourceData, databaseName string) string {
	var query string
	privileges := []string{}
	for _, p := range d.Get(grantPrivilegesAttr).(*schema.Set).List() {
		privileges = append(privileges, p.(string))
	}

	switch strings.ToUpper(d.Get(grantObjectTypeAttr).(string)) {
	case "DATABASE":
		query = fmt.Sprintf(
			"GRANT %s ON DATABASE %s TO GROUP %s",
			strings.Join(privileges, ","),
			pq.QuoteIdentifier(databaseName),
			pq.QuoteIdentifier(d.Get(grantGroupAttr).(string)),
		)
	case "SCHEMA":
		query = fmt.Sprintf(
			"GRANT %s ON SCHEMA %s TO GROUP %s",
			strings.Join(privileges, ","),
			pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
			pq.QuoteIdentifier(d.Get(grantGroupAttr).(string)),
		)
	case "TABLE":
		objects := d.Get(grantObjectsAttr).(*schema.Set)
		if objects.Len() > 0 {
			query = fmt.Sprintf(
				"GRANT %s ON %s %s TO GROUP %s",
				strings.Join(privileges, ","),
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				setToPgIdentList(objects, d.Get(grantSchemaAttr).(string)),
				pq.QuoteIdentifier(d.Get(grantGroupAttr).(string)),
			)
		} else {
			query = fmt.Sprintf(
				"GRANT %s ON ALL %sS IN SCHEMA %s TO GROUP %s",
				strings.Join(privileges, ","),
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
				pq.QuoteIdentifier(d.Get(grantGroupAttr).(string)),
			)
		}
	}

	return query
}

func generateGrantID(d *schema.ResourceData) string {
	groupName := d.Get(defaultPrivilegesGroupAttr).(string)
	objectType := d.Get(defaultPrivilegesObjectTypeAttr).(string)
	parts := []string{groupName, objectType}

	if objectType != "database" {
		parts = append(parts, d.Get(grantSchemaAttr).(string))
	}

	for _, object := range d.Get(grantObjectsAttr).(*schema.Set).List() {
		parts = append(parts, object.(string))
	}

	return strings.Join(parts, "_")
}
