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
	defaultPrivilegesGroupAttr      = "group"
	defaultPrivilegesOwnerAttr      = "owner"
	defaultPrivilegesSchemaAttr     = "schema"
	defaultPrivilegesPrivilegesAttr = "privileges"
	defaultPrivilegesObjectTypeAttr = "object_type"

	defaultPrivilegesAllSchemasID = 0
)

var defaultPrivilegesAllowedObjectTypes = []string{
	"table",
}

var defaultPrivilegesObjectTypesCodes = map[string]string{
	"table": "r",
}

func redshiftDefaultPrivileges() *schema.Resource {
	return &schema.Resource{
		Description: `Defines the default set of access privileges to be applied to objects that are created in the future by the specified user. By default, users can change only their own default access privileges. Only a superuser can specify default privileges for other users.`,
		Read:        RedshiftResourceFunc(resourceRedshiftDefaultPrivilegesRead),
		Create: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftDefaultPrivilegesCreate),
		),
		Delete: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftDefaultPrivilegesDelete),
		),
		// Since we revoke all when creating, we can use create as update
		Update: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftDefaultPrivilegesCreate),
		),

		Schema: map[string]*schema.Schema{
			defaultPrivilegesSchemaAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "The database schema to set default privileges for this group.",
			},
			defaultPrivilegesGroupAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the group to which grant default privileges on.",
			},
			defaultPrivilegesOwnerAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Target user for which to alter default privileges.",
			},
			defaultPrivilegesObjectTypeAttr: {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringInSlice(defaultPrivilegesAllowedObjectTypes, false),
				Description:  "The Redshift object type to set the default privileges on (one of: " + strings.Join(defaultPrivilegesAllowedObjectTypes, ", ") + ").",
			},
			defaultPrivilegesPrivilegesAttr: {
				Type:     schema.TypeSet,
				Required: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
					StateFunc: func(val interface{}) string {
						return strings.ToLower(val.(string))
					},
				},
				Set:         schema.HashString,
				Description: "The list of privileges to apply as default privileges. See [ALTER DEFAULT PRIVILEGES command documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_DEFAULT_PRIVILEGES.html) to see what privileges are available to which object type.",
			},
		},
	}
}

func resourceRedshiftDefaultPrivilegesDelete(db *DBConnection, d *schema.ResourceData) error {
	revokeAlterDefaultQuery := createAlterDefaultsRevokeQuery(d)

	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if _, err := tx.Exec(revokeAlterDefaultQuery); err != nil {
		return err
	}

	return tx.Commit()
}

func resourceRedshiftDefaultPrivilegesCreate(db *DBConnection, d *schema.ResourceData) error {
	privilegesSet := d.Get(defaultPrivilegesPrivilegesAttr).(*schema.Set)
	objectType := d.Get(defaultPrivilegesObjectTypeAttr).(string)

	privileges := []string{}
	for _, p := range privilegesSet.List() {
		privileges = append(privileges, strings.ToUpper(p.(string)))
	}

	if !validatePrivileges(privileges, objectType) {
		return fmt.Errorf("Invalid privileges list '%v' for object type '%s'", privileges, objectType)
	}

	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	revokeAlterDefaultQuery := createAlterDefaultsRevokeQuery(d)
	if _, err := tx.Exec(revokeAlterDefaultQuery); err != nil {
		return err
	}

	if len(privileges) > 0 {
		alterDefaultQuery := createAlterDefaultsGrantQuery(d, privileges)
		if _, err := tx.Exec(alterDefaultQuery); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	d.SetId(generateDefaultPrivilegesID(d))

	return resourceRedshiftDefaultPrivilegesReadImpl(db, d)
}

func resourceRedshiftDefaultPrivilegesRead(db *DBConnection, d *schema.ResourceData) error {
	return resourceRedshiftDefaultPrivilegesReadImpl(db, d)
}

func resourceRedshiftDefaultPrivilegesReadImpl(db *DBConnection, d *schema.ResourceData) error {
	schemaName, schemaNameSet := d.GetOk(defaultPrivilegesSchemaAttr)
	groupName := d.Get(defaultPrivilegesGroupAttr).(string)
	ownerName := d.Get(defaultPrivilegesOwnerAttr).(string)

	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	schemaID := defaultPrivilegesAllSchemasID
	if schemaNameSet {
		log.Printf("[DEBUG] getting ID for schema %s\n", schemaName)
		schemaID, err = getSchemaIDFromName(tx, schemaName.(string))
		if err != nil {
			return fmt.Errorf("failed to get schema ID for schema '%s': %w", schemaName, err)
		}
	}

	log.Printf("[DEBUG] getting ID for group %s\n", groupName)
	groupID, err := getGroupIDFromName(tx, groupName)
	if err != nil {
		return fmt.Errorf("failed to get group ID: %w", err)
	}

	log.Printf("[DEBUG] getting ID for owner %s\n", ownerName)
	ownerID, err := getUserIDFromName(tx, ownerName)
	if err != nil {
		return fmt.Errorf("failed to get user ID: %w", err)
	}

	switch strings.ToUpper(d.Get(defaultPrivilegesObjectTypeAttr).(string)) {
	case "TABLE":
		log.Println("[DEBUG] reading default privileges")
		if err := readGroupTableDefaultPrivileges(tx, d, groupID, schemaID, ownerID); err != nil {
			return fmt.Errorf("failed to read table privileges: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}

func readGroupTableDefaultPrivileges(tx *sql.Tx, d *schema.ResourceData, groupID, schemaID, ownerID int) error {
	var tableSelect, tableUpdate, tableInsert, tableDelete, tableReferences bool
	tableDefaultPrivilegeQuery := `
	      SELECT 
		decode(charindex('r',split_part(split_part(array_to_string(defaclacl, '|'),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) as select,
		decode(charindex('w',split_part(split_part(array_to_string(defaclacl, '|'),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) as update,
		decode(charindex('a',split_part(split_part(array_to_string(defaclacl, '|'),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) as insert,
		decode(charindex('d',split_part(split_part(array_to_string(defaclacl, '|'),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) as delete,
		decode(charindex('x',split_part(split_part(array_to_string(defaclacl, '|'),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) as references
	      FROM pg_group gr, pg_default_acl acl
	      WHERE 
		acl.defaclnamespace = $1
		AND array_to_string(acl.defaclacl, '|') LIKE '%' || 'group ' || gr.groname || '=%'
		AND gr.grosysid = $2
		AND acl.defaclobjtype = $3
		AND acl.defacluser = $4`

	if err := tx.QueryRow(tableDefaultPrivilegeQuery, schemaID, groupID, defaultPrivilegesObjectTypesCodes["table"], ownerID).Scan(
		&tableSelect,
		&tableUpdate,
		&tableInsert,
		&tableDelete,
		&tableReferences); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to collect group privileges: %w", err)
	}

	privileges := []string{}
	appendIfTrue(tableSelect, "select", &privileges)
	appendIfTrue(tableUpdate, "update", &privileges)
	appendIfTrue(tableInsert, "insert", &privileges)
	appendIfTrue(tableDelete, "delete", &privileges)
	appendIfTrue(tableReferences, "references", &privileges)

	log.Printf("[DEBUG] Collected privileges for group ID %d: %v\n", groupID, privileges)

	d.Set(defaultPrivilegesPrivilegesAttr, privileges)

	return nil
}

func generateDefaultPrivilegesID(d *schema.ResourceData) string {
	schemaName, schemaNameSet := d.GetOk(defaultPrivilegesSchemaAttr)

	groupName := d.Get(defaultPrivilegesGroupAttr).(string)
	ownerName := d.Get(defaultPrivilegesOwnerAttr).(string)
	objectType := d.Get(defaultPrivilegesObjectTypeAttr).(string)

	if !schemaNameSet {
		schemaName = "noschema"
	}

	return strings.Join([]string{
		groupName, schemaName.(string), ownerName, objectType,
	}, "_")
}

func createAlterDefaultsGrantQuery(d *schema.ResourceData, privileges []string) string {
	schemaName, schemaNameSet := d.GetOk(defaultPrivilegesSchemaAttr)
	groupName := d.Get(defaultPrivilegesGroupAttr).(string)
	ownerName := d.Get(defaultPrivilegesOwnerAttr).(string)
	objectType := strings.ToUpper(d.Get(defaultPrivilegesObjectTypeAttr).(string))

	alterQuery := fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s", pq.QuoteIdentifier(ownerName))

	if schemaNameSet {
		alterQuery = fmt.Sprintf("%s IN SCHEMA %s", alterQuery, pq.QuoteIdentifier(schemaName.(string)))
	}

	return fmt.Sprintf(
		"%s GRANT %s ON %sS TO GROUP %s",
		alterQuery,
		strings.Join(privileges, ","),
		objectType,
		pq.QuoteIdentifier(groupName),
	)
}

func createAlterDefaultsRevokeQuery(d *schema.ResourceData) string {
	schemaName, schemaNameSet := d.GetOk(defaultPrivilegesSchemaAttr)
	groupName := d.Get(defaultPrivilegesGroupAttr).(string)
	ownerName := d.Get(defaultPrivilegesOwnerAttr).(string)
	objectType := strings.ToUpper(d.Get(defaultPrivilegesObjectTypeAttr).(string))

	alterQuery := fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s", pq.QuoteIdentifier(ownerName))

	if schemaNameSet {
		alterQuery = fmt.Sprintf("%s IN SCHEMA %s", alterQuery, pq.QuoteIdentifier(schemaName.(string)))
	}

	return fmt.Sprintf(
		"%s REVOKE ALL PRIVILEGES ON %sS FROM GROUP %s",
		alterQuery,
		objectType,
		pq.QuoteIdentifier(groupName),
	)
}
