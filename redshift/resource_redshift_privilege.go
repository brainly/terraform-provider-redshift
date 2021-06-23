package redshift

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	privilegeGroupAttr           = "group"
	privilegeSchemaAttr          = "schema"
	privilegePrivilegesAttr      = "privileges"
	privilegeObjectTypeAttr      = "object_type"
	privilegeWithGrantOptionAttr = "with_grant_option"
)

var allowedObjectTypes = []string{
	"schema",
	"table",
}

var objectTypesCodes = map[string]string{
	"table":    "r",
	"sequence": "S",
	"function": "f",
	"type":     "T",
}

func redshiftPrivilege() *schema.Resource {
	return &schema.Resource{
		Description: `
When you create a database object, you are its owner. By default, only a superuser or the owner of an object can query, modify, or grant privileges on the object. For users to use an object, you must grant the necessary privileges to the user or the group that contains the user. Database superusers have the same privileges as database owners.
`,
		Read: RedshiftResourceFunc(resourceRedshiftPrivilegeRead),
		Delete: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftPrivilegeDelete),
		),
		Create: RedshiftResourceFunc(resourceRedshiftPrivilegeCreate),
		// Since we revoke all when creating, we can use create as update
		Update: RedshiftResourceFunc(resourceRedshiftPrivilegeCreate),

		Schema: map[string]*schema.Schema{
			privilegeSchemaAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "The database schema to grant privileges on for this group.",
			},
			privilegeGroupAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the group to grant privileges on.",
			},
			privilegeObjectTypeAttr: {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringInSlice(allowedObjectTypes, false),
				Description:  "The Redshift object type to grant the privileges on (one of: " + strings.Join(allowedObjectTypes, ", ") + ").",
			},
			privilegePrivilegesAttr: &schema.Schema{
				Type:        schema.TypeSet,
				Required:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Set:         schema.HashString,
				Description: "The list of privileges to grant. See [GRANT SQL command documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html) to see what privileges are available to which object type.",
			},
		},
	}
}

func resourceRedshiftPrivilegeDelete(db *DBConnection, d *schema.ResourceData) error {
	revokeQuery, revokeAlterDefaultQuery := createRevokeQuery(d)

	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := execQueryIfNotEmpty(tx, revokeQuery); err != nil {
		return err
	}

	if err := execQueryIfNotEmpty(tx, revokeAlterDefaultQuery); err != nil {
		return err
	}

	return tx.Commit()
}

func resourceRedshiftPrivilegeCreate(db *DBConnection, d *schema.ResourceData) error {
	privilegesSet := d.Get(privilegePrivilegesAttr).(*schema.Set)
	objectType := d.Get(privilegeObjectTypeAttr).(string)

	privileges := []string{}
	for _, p := range privilegesSet.List() {
		privileges = append(privileges, strings.ToUpper(p.(string)))
	}

	if !validatePrivileges(privileges, objectType) {
		return fmt.Errorf("Invalid privileges list '%v' for object type '%s'", privileges, objectType)
	}

	revokeQuery, revokeAlterDefaultQuery := createRevokeQuery(d)
	grantQuery, alterDefaultQuery := createGrantQuery(d, privileges)

	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := execQueryIfNotEmpty(tx, revokeQuery); err != nil {
		return fmt.Errorf("failed to revoke privileges for group '%s': %w", revokeQuery, err)
	}

	if err := execQueryIfNotEmpty(tx, revokeAlterDefaultQuery); err != nil {
		return fmt.Errorf("failed to revoke default privileges for group '%s': %w", revokeAlterDefaultQuery, err)
	}

	if err := execQueryIfNotEmpty(tx, grantQuery); err != nil {
		return fmt.Errorf("failed to grant privileges: %w", err)
	}

	if err := execQueryIfNotEmpty(tx, alterDefaultQuery); err != nil {
		return fmt.Errorf("failed to grant default privileges: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	d.SetId(generatePrivilegesID(d))

	return resourceRedshiftPrivilegeReadImpl(db, d)
}

func resourceRedshiftPrivilegeRead(db *DBConnection, d *schema.ResourceData) error {
	return resourceRedshiftPrivilegeReadImpl(db, d)
}

func resourceRedshiftPrivilegeReadImpl(db *DBConnection, d *schema.ResourceData) error {
	schemaName := d.Get(privilegeSchemaAttr).(string)
	groupName := d.Get(privilegeGroupAttr).(string)

	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	schemaID, err := getSchemaIDFromName(tx, schemaName)
	if err != nil {
		return fmt.Errorf("failed to get schema ID: %w", err)
	}

	groupID, err := getGroupIDFromName(tx, groupName)
	if err != nil {
		return fmt.Errorf("failed to get group ID: %w", err)
	}

	switch strings.ToUpper(d.Get(privilegeObjectTypeAttr).(string)) {
	case "TABLE":
		if err := readGroupTablePrivileges(tx, d, groupID, schemaID); err != nil {
			return fmt.Errorf("failed to read table privileges: %w", err)
		}

	case "SCHEMA":
		if err := readGroupSchemaPrivileges(tx, d, groupID, schemaID); err != nil {
			return fmt.Errorf("failed to read schema privileges: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}

func readGroupSchemaPrivileges(tx *sql.Tx, d *schema.ResourceData, groupID, schemaID int) error {
	var schemaUsage, schemaCreate bool
	schemaPrivilegeQuery := `
	      SELECT
		CASE
		  WHEN charindex('U',split_part(split_part(array_to_string(nspacl, '|'), 'group ' || gr.groname,2 ) ,'/',1)) > 0 THEN 1
		  ELSE 0
		END AS usage,
		CASE
		  WHEN charindex('C',split_part(split_part(array_to_string(nspacl, '|'),'group ' || gr.groname,2 ) ,'/',1)) > 0 THEN 1
		  ELSE 0
		END AS create
	      FROM pg_group gr, pg_namespace ns
	      WHERE 
		array_to_string(ns.nspacl, '|') LIKE '%' || 'group ' || gr.groname || '=%'
		AND ns.oid = $1
		AND gr.grosysid = $2`

	if err := tx.QueryRow(schemaPrivilegeQuery, schemaID, groupID).Scan(&schemaUsage, &schemaCreate); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to collect group privileges: %w", err)
	}

	privileges := []string{}
	appendIfTrue(schemaUsage, "usage", &privileges)
	appendIfTrue(schemaCreate, "create", &privileges)

	d.Set(privilegePrivilegesAttr, privileges)

	return nil
}

func readGroupTablePrivileges(tx *sql.Tx, d *schema.ResourceData, groupID, schemaID int) error {
	var tableSelect, tableUpdate, tableInsert, tableDelete, tableReferences bool
	tableDefaultPrivilegeQuery := `
	      SELECT 
		decode(charindex('r',split_part(split_part(array_to_string(defaclacl, '|'),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) as select,
		decode(charindex('w',split_part(split_part(array_to_string(defaclacl, '|'),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) as update,
		decode(charindex('a',split_part(split_part(array_to_string(defaclacl, '|'),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) as insert,
		decode(charindex('d',split_part(split_part(array_to_string(defaclacl, '|'),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) as delete,
		decode(charindex('x',split_part(split_part(array_to_string(defaclacl, '|'),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) as references
	      FROM pg_group gr, pg_default_acl acl, pg_namespace ns
	      WHERE 
		acl.defaclnamespace = ns.oid 
		AND array_to_string(acl.defaclacl, '|') LIKE '%' || 'group ' || gr.groname || '=%'
		AND ns.oid = $1
		AND gr.grosysid = $2
		AND acl.defaclobjtype = $3`

	if err := tx.QueryRow(tableDefaultPrivilegeQuery, schemaID, groupID, objectTypesCodes["table"]).Scan(&tableSelect, &tableUpdate, &tableInsert, &tableDelete, &tableReferences); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to collect group privileges: %w", err)
	}

	privileges := []string{}
	appendIfTrue(tableSelect, "select", &privileges)
	appendIfTrue(tableUpdate, "update", &privileges)
	appendIfTrue(tableInsert, "insert", &privileges)
	appendIfTrue(tableDelete, "delete", &privileges)
	appendIfTrue(tableReferences, "references", &privileges)

	d.Set(privilegePrivilegesAttr, privileges)

	return nil
}

func appendIfTrue(condition bool, item string, list *[]string) {
	if condition {
		*list = append(*list, item)
	}
}

func generatePrivilegesID(d *schema.ResourceData) string {
	schemaName := d.Get(privilegeSchemaAttr).(string)
	groupName := d.Get(privilegeGroupAttr).(string)
	objectType := d.Get(privilegeObjectTypeAttr).(string)

	return strings.Join([]string{groupName, schemaName, objectType}, "_")
}

func createGrantQuery(d *schema.ResourceData, privileges []string) (grantQuery string, alterDefaultQuery string) {
	if len(privileges) == 0 {
		return
	}

	schemaName := d.Get(privilegeSchemaAttr).(string)
	groupName := d.Get(privilegeGroupAttr).(string)

	switch strings.ToUpper(d.Get(privilegeObjectTypeAttr).(string)) {
	case "SCHEMA":
		grantQuery = fmt.Sprintf(
			"GRANT %s ON SCHEMA %s TO GROUP %s",
			strings.Join(privileges, ","),
			pq.QuoteIdentifier(schemaName),
			pq.QuoteIdentifier(groupName),
		)
	case "TABLE":
		grantQuery = fmt.Sprintf(
			"GRANT %s ON ALL TABLES IN SCHEMA %s TO GROUP %s",
			strings.Join(privileges, ","),
			pq.QuoteIdentifier(schemaName),
			pq.QuoteIdentifier(groupName),
		)
		alterDefaultQuery = fmt.Sprintf(
			"ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT %s ON TABLES TO GROUP %s",
			pq.QuoteIdentifier(schemaName),
			strings.Join(privileges, ","),
			pq.QuoteIdentifier(groupName),
		)
	}

	return
}

func createRevokeQuery(d *schema.ResourceData) (revokeQuery string, alterDefaultQuery string) {
	schemaName := d.Get(privilegeSchemaAttr).(string)
	groupName := d.Get(privilegeGroupAttr).(string)

	switch strings.ToUpper(d.Get(privilegeObjectTypeAttr).(string)) {
	case "SCHEMA":
		revokeQuery = fmt.Sprintf(
			"REVOKE ALL PRIVILEGES ON SCHEMA %s FROM GROUP %s",
			pq.QuoteIdentifier(schemaName),
			pq.QuoteIdentifier(groupName),
		)
	case "TABLE":
		revokeQuery = fmt.Sprintf(
			"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s FROM GROUP %s",
			pq.QuoteIdentifier(schemaName),
			pq.QuoteIdentifier(groupName),
		)
		alterDefaultQuery = fmt.Sprintf(
			"ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE ALL PRIVILEGES ON TABLES FROM GROUP %s",
			pq.QuoteIdentifier(schemaName),
			pq.QuoteIdentifier(groupName),
		)
	}

	return
}

func execQueryIfNotEmpty(tx *sql.Tx, query string) error {
	if query == "" {
		return nil
	}

	_, err := tx.Exec(query)
	return err
}

func validatePrivileges(privileges []string, objectType string) bool {
	for _, p := range privileges {
		switch strings.ToUpper(objectType) {
		case "SCHEMA":
			switch strings.ToUpper(p) {
			case "CREATE", "USAGE":
				continue
			default:
				return false
			}
		case "TABLE":
			switch strings.ToUpper(p) {
			case "SELECT", "UPDATE", "INSERT", "DELETE", "REFERENCES":
				continue
			default:
				return false
			}
		}

	}

	return true
}
