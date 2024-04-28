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
	defaultPrivilegesUserAttr       = "user"
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
		ReadContext: RedshiftResourceFunc(resourceRedshiftDefaultPrivilegesRead),
		CreateContext: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftDefaultPrivilegesCreate),
		),
		DeleteContext: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftDefaultPrivilegesDelete),
		),
		// Since we revoke all when creating, we can use create as update
		UpdateContext: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftDefaultPrivilegesCreate),
		),

		Schema: map[string]*schema.Schema{
			defaultPrivilegesSchemaAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "If set, the specified default privileges are applied to new objects created in the specified schema. In this case, the user or user group that is the target of ALTER DEFAULT PRIVILEGES must have CREATE privilege for the specified schema. Default privileges that are specific to a schema are added to existing global default privileges. By default, default privileges are applied globally to the entire database.",
			},
			defaultPrivilegesGroupAttr: {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ExactlyOneOf: []string{defaultPrivilegesGroupAttr, defaultPrivilegesUserAttr},
				Description:  "The name of the  group to which the specified default privileges are applied.",
			},
			defaultPrivilegesUserAttr: {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ExactlyOneOf: []string{defaultPrivilegesGroupAttr, defaultPrivilegesUserAttr},
				Description:  "The name of the user to which the specified default privileges are applied.",
			},
			defaultPrivilegesOwnerAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the user for which default privileges are defined. Only a superuser can specify default privileges for other users.",
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
	var entityID int
	var entityIsUser bool
	schemaName, schemaNameSet := d.GetOk(defaultPrivilegesSchemaAttr)
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

	if groupName, groupNameSet := d.GetOk(defaultPrivilegesGroupAttr); groupNameSet {
		log.Printf("[DEBUG] getting ID for group %s\n", groupName.(string))
		entityID, err = getGroupIDFromName(tx, groupName.(string))
		entityIsUser = false
		if err != nil {
			return fmt.Errorf("failed to get group ID: %w", err)
		}
	} else if userName, userNameSet := d.GetOk(defaultPrivilegesUserAttr); userNameSet {
		log.Printf("[DEBUG] getting ID for user %s\n", userName.(string))
		entityID, err = getUserIDFromName(tx, userName.(string))
		entityIsUser = true
		if err != nil {
			return fmt.Errorf("failed to get user ID: %w", err)
		}
	}

	log.Printf("[DEBUG] getting ID for owner %s\n", ownerName)
	ownerID, err := getUserIDFromName(tx, ownerName)
	if err != nil {
		return fmt.Errorf("failed to get user ID: %w", err)
	}

	switch strings.ToUpper(d.Get(defaultPrivilegesObjectTypeAttr).(string)) {
	case "TABLE":
		log.Println("[DEBUG] reading default privileges")
		if err := readGroupTableDefaultPrivileges(tx, d, entityID, schemaID, ownerID, entityIsUser); err != nil {
			return fmt.Errorf("failed to read table privileges: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}

func readGroupTableDefaultPrivileges(tx *sql.Tx, d *schema.ResourceData, entityID, schemaID, ownerID int, entityIsUser bool) error {
	var tableSelect, tableUpdate, tableInsert, tableDelete, tableDrop, tableReferences, tableRule, tableTrigger bool
	var query string

	if entityIsUser {
		query = `
	      SELECT 
		decode(charindex('r',split_part(split_part(regexp_replace(replace(array_to_string(defaclacl, '|'), '"', ''), 'group '||u.usename), u.usename||'=', 2) ,'/',1)),0,0,1) AS SELECT,
		decode(charindex('w',split_part(split_part(regexp_replace(replace(array_to_string(defaclacl, '|'), '"', ''), 'group '||u.usename), u.usename||'=', 2) ,'/',1)),0,0,1) AS UPDATE,
		decode(charindex('a',split_part(split_part(regexp_replace(replace(array_to_string(defaclacl, '|'), '"', ''), 'group '||u.usename), u.usename||'=', 2) ,'/',1)),0,0,1) AS INSERT,
		decode(charindex('d',split_part(split_part(regexp_replace(replace(array_to_string(defaclacl, '|'), '"', ''), 'group '||u.usename), u.usename||'=', 2) ,'/',1)),0,0,1) AS DELETE,
		decode(charindex('D',split_part(split_part(regexp_replace(replace(array_to_string(defaclacl, '|'), '"', ''), 'group '||u.usename), u.usename||'=', 2) ,'/',1)),0,0,1) AS DROP,
		decode(charindex('x',split_part(split_part(regexp_replace(replace(array_to_string(defaclacl, '|'), '"', ''), 'group '||u.usename), u.usename||'=', 2) ,'/',1)),0,0,1) AS REFERENCES,
		decode(charindex('R',split_part(split_part(regexp_replace(replace(array_to_string(defaclacl, '|'), '"', ''), 'group '||u.usename), u.usename||'=', 2) ,'/',1)),0,0,1) AS rule,
		decode(charindex('t',split_part(split_part(regexp_replace(replace(array_to_string(defaclacl, '|'), '"', ''), 'group '||u.usename), u.usename||'=', 2) ,'/',1)),0,0,1) AS TRIGGER
	      FROM pg_user u, pg_default_acl acl
	      WHERE 
		acl.defaclnamespace = $1
		AND regexp_replace(replace(array_to_string(acl.defaclacl, '|'), '"', ''), 'group '||u.usename) LIKE '%' || u.usename || '=%'
		AND u.usesysid = $2
		AND acl.defaclobjtype = $3
		AND acl.defacluser = $4
		`
	} else {
		query = `
	      SELECT 
		decode(charindex('r',split_part(split_part(replace(array_to_string(defaclacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) AS SELECT,
		decode(charindex('w',split_part(split_part(replace(array_to_string(defaclacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) AS UPDATE,
		decode(charindex('a',split_part(split_part(replace(array_to_string(defaclacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) AS INSERT,
		decode(charindex('d',split_part(split_part(replace(array_to_string(defaclacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) AS DELETE,
		decode(charindex('D',split_part(split_part(replace(array_to_string(defaclacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) AS DROP,
		decode(charindex('x',split_part(split_part(replace(array_to_string(defaclacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) AS REFERENCES,
		decode(charindex('R',split_part(split_part(replace(array_to_string(defaclacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) AS rule,
		decode(charindex('t',split_part(split_part(replace(array_to_string(defaclacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)),0,0,1) AS TRIGGER
	      FROM pg_group gr, pg_default_acl acl
	      WHERE 
		acl.defaclnamespace = $1
		AND replace(array_to_string(acl.defaclacl, '|'), '"', '') LIKE '%' || 'group ' || gr.groname || '=%'
		AND gr.grosysid = $2
		AND acl.defaclobjtype = $3
		AND acl.defacluser = $4
		`
	}

	if err := tx.QueryRow(query, schemaID, entityID, defaultPrivilegesObjectTypesCodes["table"], ownerID).Scan(
		&tableSelect,
		&tableUpdate,
		&tableInsert,
		&tableDelete,
		&tableDrop,
		&tableReferences,
		&tableRule,
		&tableTrigger); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to collect privileges: %w", err)
	}

	privileges := []string{}
	appendIfTrue(tableSelect, "select", &privileges)
	appendIfTrue(tableUpdate, "update", &privileges)
	appendIfTrue(tableInsert, "insert", &privileges)
	appendIfTrue(tableDelete, "delete", &privileges)
	appendIfTrue(tableDrop, "drop", &privileges)
	appendIfTrue(tableReferences, "references", &privileges)
	appendIfTrue(tableRule, "rule", &privileges)
	appendIfTrue(tableTrigger, "trigger", &privileges)

	log.Printf("[DEBUG] Collected privileges for ID %d: %v\n", entityID, privileges)

	d.Set(defaultPrivilegesPrivilegesAttr, privileges)

	return nil
}

func generateDefaultPrivilegesID(d *schema.ResourceData) string {
	var entityName, schemaName string

	if groupName, isGroup := d.GetOk(defaultPrivilegesGroupAttr); isGroup {
		entityName = fmt.Sprintf("gn:%s", groupName.(string))
	} else if userName, isUser := d.GetOk(defaultPrivilegesUserAttr); isUser {
		entityName = fmt.Sprintf("un:%s", userName.(string))
	}

	if schemaNameRaw, schemaNameSet := d.GetOk(defaultPrivilegesSchemaAttr); schemaNameSet {
		schemaName = fmt.Sprintf("sn:%s", schemaNameRaw.(string))
	} else {
		schemaName = "noschema"
	}

	ownerName := fmt.Sprintf("on:%s", d.Get(defaultPrivilegesOwnerAttr).(string))
	objectType := fmt.Sprintf("ot:%s", d.Get(defaultPrivilegesObjectTypeAttr).(string))

	return strings.Join([]string{
		entityName, schemaName, ownerName, objectType,
	}, "_")
}

func createAlterDefaultsGrantQuery(d *schema.ResourceData, privileges []string) string {
	schemaName, schemaNameSet := d.GetOk(defaultPrivilegesSchemaAttr)
	ownerName := d.Get(defaultPrivilegesOwnerAttr).(string)
	objectType := strings.ToUpper(d.Get(defaultPrivilegesObjectTypeAttr).(string))

	var entityName, toWhomIndicator string
	if groupName, isGroup := d.GetOk(defaultPrivilegesGroupAttr); isGroup {
		entityName = groupName.(string)
		toWhomIndicator = "GROUP"
	} else if userName, isUser := d.GetOk(defaultPrivilegesUserAttr); isUser {
		entityName = userName.(string)
	}

	alterQuery := fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s", pq.QuoteIdentifier(ownerName))

	if schemaNameSet {
		alterQuery = fmt.Sprintf("%s IN SCHEMA %s", alterQuery, pq.QuoteIdentifier(schemaName.(string)))
	}

	return fmt.Sprintf(
		"%s GRANT %s ON %sS TO %s %s",
		alterQuery,
		strings.Join(privileges, ","),
		objectType,
		toWhomIndicator,
		pq.QuoteIdentifier(entityName),
	)
}

func createAlterDefaultsRevokeQuery(d *schema.ResourceData) string {
	schemaName, schemaNameSet := d.GetOk(defaultPrivilegesSchemaAttr)
	ownerName := d.Get(defaultPrivilegesOwnerAttr).(string)
	objectType := strings.ToUpper(d.Get(defaultPrivilegesObjectTypeAttr).(string))

	var entityName, fromWhomIndicator string
	if groupName, isGroup := d.GetOk(defaultPrivilegesGroupAttr); isGroup {
		entityName = groupName.(string)
		fromWhomIndicator = "GROUP"
	} else if userName, isUser := d.GetOk(defaultPrivilegesUserAttr); isUser {
		entityName = userName.(string)
	}

	alterQuery := fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s", pq.QuoteIdentifier(ownerName))

	if schemaNameSet {
		alterQuery = fmt.Sprintf("%s IN SCHEMA %s", alterQuery, pq.QuoteIdentifier(schemaName.(string)))
	}

	return fmt.Sprintf(
		"%s REVOKE ALL PRIVILEGES ON %sS FROM %s %s",
		alterQuery,
		objectType,
		fromWhomIndicator,
		pq.QuoteIdentifier(entityName),
	)
}
