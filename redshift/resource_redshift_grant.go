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
	grantUserAttr       = "user"
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
	"datashare_database",
	"function",
	"procedure",
	"language",
}

var grantObjectTypesCodes = map[string][]string{
	"table":     {"r", "m", "v"},
	"procedure": {"p"},
	"function":  {"f"},
}

func redshiftGrant() *schema.Resource {
	return &schema.Resource{
		Description: `
Defines access privileges for users and  groups. Privileges include access options such as being able to read data in tables and views, write data, create tables, and drop tables. Use this command to give specific privileges for a table, database, datashare_database, schema, function, procedure, language, or column.
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
			grantUserAttr: {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ExactlyOneOf: []string{grantUserAttr, grantGroupAttr},
				Description:  "The name of the user to grant privileges on. Either `user` or `group` parameter must be set.",
			},
			grantGroupAttr: {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ExactlyOneOf: []string{grantUserAttr, grantGroupAttr},
				Description:  "The name of the group to grant privileges on. Either `group` or `user` parameter must be set.",
			},
			grantSchemaAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "The database schema to grant privileges on.",
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
				Description: "The objects upon which to grant the privileges. An empty list (the default) means to grant permissions on all objects of the specified type. Ignored when `object_type` is one of (`schema`).  There is a current limitation that if `object_type` is one of (`database`,`datashare_database`), the list can only have a single element, i.e. as a part of one resource, grants for only one `database` or `datashare_database` can be created. If you want to add grants for multiple, you can create them in separate resources",
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
				Description: "The list of privileges to apply as default privileges. See [GRANT command documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html) to see what privileges are available to which object type. An empty list could be provided to revoke all privileges for this user or group. Required when `object_type` is set to `language`.",
			},
		},
	}
}

func resourceRedshiftGrantCreate(db *DBConnection, d *schema.ResourceData) error {
	objectType := d.Get(grantObjectTypeAttr).(string)
	schemaName := d.Get(grantSchemaAttr).(string)
	var databaseName string

	privileges := []string{}
	for _, p := range d.Get(grantPrivilegesAttr).(*schema.Set).List() {
		privileges = append(privileges, p.(string))
	}

	objects := []string{}
	for _, p := range d.Get(grantObjectsAttr).(*schema.Set).List() {
		objects = append(objects, p.(string))
	}

	// validate parameters
	if (objectType == "table" || objectType == "function" || objectType == "procedure") && schemaName == "" {
		return fmt.Errorf("parameter `%s` is required for objects of type table, function and procedure", grantSchemaAttr)
	}

	if (objectType == "schema") && len(objects) > 0 {
		return fmt.Errorf("cannot specify `%s` when `%s` is `database` or `schema`", grantObjectsAttr, grantObjectTypeAttr)
	}

	if objectType == "language" && len(objects) == 0 {
		return fmt.Errorf("parameter `%s` is required for objects of type language", grantObjectsAttr)
	}

	if (objectType == "datashare_database" || objectType == "database") && len(objects) > 1 {
		return fmt.Errorf("parameter `%s` can have only one value when `%s` is one of (`database`,`datashare_database`)", grantObjectsAttr, grantObjectTypeAttr)
	}

	if !validatePrivileges(privileges, objectType) {
		return fmt.Errorf("Invalid privileges list %v for object of type %s", privileges, objectType)
	}

	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if objectType == "datashare_database" || objectType == "database" {
		databaseName = objects[0]
	} else {
		databaseName = db.client.databaseName
	}

	if err := revokeGrants(tx, databaseName, d); err != nil {
		return err
	}

	if err := createGrants(tx, databaseName, d); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	d.SetId(generateGrantID(d))

	return resourceRedshiftGrantReadImpl(db, d)
}

func resourceRedshiftGrantDelete(db *DBConnection, d *schema.ResourceData) error {

	objectType := d.Get(grantObjectTypeAttr).(string)
	objects := []string{}
	for _, p := range d.Get(grantObjectsAttr).(*schema.Set).List() {
		objects = append(objects, p.(string))
	}
	var databaseName string
	if objectType == "datashare_database" || objectType == "database" {
		databaseName = objects[0]
	} else {
		databaseName = db.client.databaseName
	}

	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := revokeGrants(tx, databaseName, d); err != nil {
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
	case "database", "datashare_database":
		return readDatabaseGrants(db, d)
	case "schema":
		return readSchemaGrants(db, d)
	case "table":
		return readTableGrants(db, d)
	case "function", "procedure":
		return readCallableGrants(db, d)
	case "language":
		return readLanguageGrants(db, d)
	default:
		return fmt.Errorf("Unsupported %s %s", grantObjectTypeAttr, objectType)
	}
}

func readDatabaseGrants(db *DBConnection, d *schema.ResourceData) error {
	var entityName, query string
	var databaseCreate, databaseTemp, databaseUsage bool

	objectType := d.Get(grantObjectTypeAttr).(string)
	objects := []string{}
	for _, p := range d.Get(grantObjectsAttr).(*schema.Set).List() {
		objects = append(objects, p.(string))
	}
	var databaseName string
	if objectType == "datashare_database" || objectType == "database" {
		databaseName = objects[0]
	} else {
		databaseName = db.client.databaseName
	}

	_, isUser := d.GetOk(grantUserAttr)

	if isUser {
		entityName = d.Get(grantUserAttr).(string)
		query = `
  SELECT
    decode(charindex('C',split_part(split_part(regexp_replace(replace(array_to_string(db.datacl, '|'), '"', ''),'group '||u.usename,'__avoidGroupPrivs__'), u.usename||'=', 2) ,'/',1)), 0,0,1) as create,
    decode(charindex('T',split_part(split_part(regexp_replace(replace(array_to_string(db.datacl, '|'), '"', ''),'group '||u.usename,'__avoidGroupPrivs__'), u.usename||'=', 2) ,'/',1)), 0,0,1) as temporary,
    decode(charindex('U',split_part(split_part(regexp_replace(replace(array_to_string(db.datacl, '|'), '"', ''),'group '||u.usename,'__avoidGroupPrivs__'), u.usename||'=', 2) ,'/',1)), 0,0,1) as usage
  FROM pg_database db, pg_user u
  WHERE
    db.datname=$1 
    AND u.usename=$2
`
	} else {
		entityName = d.Get(grantGroupAttr).(string)
		query = `
  SELECT
    decode(charindex('C',split_part(split_part(replace(array_to_string(db.datacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)), 0,0,1) as create,
    decode(charindex('T',split_part(split_part(replace(array_to_string(db.datacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)), 0,0,1) as temporary,
    decode(charindex('U',split_part(split_part(replace(array_to_string(db.datacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)), 0,0,1) as usage
  FROM pg_database db, pg_group gr
  WHERE
    db.datname=$1 
    AND gr.groname=$2
`
	}

	if err := db.QueryRow(query, databaseName, entityName).Scan(&databaseCreate, &databaseTemp, &databaseUsage); err != nil {
		return err
	}

	privileges := []string{}
	appendIfTrue(databaseCreate, "create", &privileges)
	appendIfTrue(databaseTemp, "temporary", &privileges)
	appendIfTrue(databaseUsage, "usage", &privileges)

	log.Printf("[DEBUG] Collected database '%s' privileges for %s: %v", databaseName, entityName, privileges)

	d.Set(grantPrivilegesAttr, privileges)

	return nil
}

func readSchemaGrants(db *DBConnection, d *schema.ResourceData) error {
	var entityName, query string
	var schemaCreate, schemaUsage bool

	_, isUser := d.GetOk(grantUserAttr)
	schemaName := d.Get(grantSchemaAttr).(string)

	if isUser {
		entityName = d.Get(grantUserAttr).(string)
		query = `
  SELECT
    decode(charindex('C',split_part(split_part(regexp_replace(replace(array_to_string(ns.nspacl, '|'), '"', ''),'group '||u.usename,'__avoidGroupPrivs__'), u.usename||'=', 2) ,'/',1)), 0,0,1) as create,
    decode(charindex('U',split_part(split_part(regexp_replace(replace(array_to_string(ns.nspacl, '|'), '"', ''),'group '||u.usename,'__avoidGroupPrivs__'), u.usename||'=', 2) ,'/',1)), 0,0,1) as usage
  FROM pg_namespace ns, pg_user u
  WHERE
    ns.nspname=$1 
    AND u.usename=$2
`
	} else {
		entityName = d.Get(grantGroupAttr).(string)
		query = `
  SELECT
    decode(charindex('C',split_part(split_part(replace(array_to_string(ns.nspacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), 0,0,1) as create,
    decode(charindex('U',split_part(split_part(replace(array_to_string(ns.nspacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), 0,0,1) as usage
  FROM pg_namespace ns, pg_group gr
  WHERE
    ns.nspname=$1 
    AND gr.groname=$2
`
	}

	if err := db.QueryRow(query, schemaName, entityName).Scan(&schemaCreate, &schemaUsage); err != nil {
		return err
	}

	privileges := []string{}
	appendIfTrue(schemaCreate, "create", &privileges)
	appendIfTrue(schemaUsage, "usage", &privileges)

	log.Printf("[DEBUG] Collected schema '%s' privileges for  %s: %v", schemaName, entityName, privileges)

	d.Set(grantPrivilegesAttr, privileges)

	return nil
}

func readTableGrants(db *DBConnection, d *schema.ResourceData) error {
	log.Printf("[DEBUG] Reading table grants")
	var entityName, query string
	_, isUser := d.GetOk(grantUserAttr)

	if isUser {
		entityName = d.Get(grantUserAttr).(string)
		query = `
  SELECT
    relname,
    decode(charindex('r',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as select,
    decode(charindex('w',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as update,
    decode(charindex('a',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as insert,
    decode(charindex('d',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as delete,
    decode(charindex('D',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as drop,
    decode(charindex('x',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as references,
    decode(charindex('R',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as rule,
    decode(charindex('t',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as trigger
  FROM pg_user u, pg_class cl
  JOIN pg_namespace nsp ON nsp.oid = cl.relnamespace
  WHERE
    cl.relkind = ANY($1)
    AND u.usename=$2
    AND nsp.nspname=$3
`
	} else {
		entityName = d.Get(grantGroupAttr).(string)
		query = `
  SELECT
    relname,
    decode(charindex('r',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as select,
    decode(charindex('w',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as update,
    decode(charindex('a',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as insert,
    decode(charindex('d',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as delete,
    decode(charindex('D',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as drop,
    decode(charindex('x',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as references,
    decode(charindex('R',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as rule,
    decode(charindex('t',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as trigger
  FROM pg_group gr, pg_class cl
  JOIN pg_namespace nsp ON nsp.oid = cl.relnamespace
  WHERE
    cl.relkind = ANY($1)
    AND gr.groname=$2
    AND nsp.nspname=$3
`
	}

	schemaName := d.Get(grantSchemaAttr).(string)
	objects := d.Get(grantObjectsAttr).(*schema.Set)

	rows, err := db.Query(query, pq.Array(grantObjectTypesCodes["table"]), entityName, schemaName)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var objName string
		var tableSelect, tableUpdate, tableInsert, tableDelete, tableDrop, tableReferences, tableRule, tableTrigger bool

		if err := rows.Scan(&objName, &tableSelect, &tableUpdate, &tableInsert, &tableDelete, &tableDrop, &tableReferences, &tableRule, &tableTrigger); err != nil {
			return err
		}

		if objects.Len() > 0 && !objects.Contains(objName) {
			continue
		}

		privilegesSet := schema.NewSet(schema.HashString, nil)
		if tableSelect {
			privilegesSet.Add("select")
		}
		if tableUpdate {
			privilegesSet.Add("update")
		}
		if tableInsert {
			privilegesSet.Add("insert")
		}
		if tableDelete {
			privilegesSet.Add("delete")
		}
		if tableDrop {
			privilegesSet.Add("drop")
		}
		if tableReferences {
			privilegesSet.Add("references")
		}
		if tableRule {
			privilegesSet.Add("rule")
		}
		if tableTrigger {
			privilegesSet.Add("trigger")
		}

		if !privilegesSet.Equal(d.Get(grantPrivilegesAttr).(*schema.Set)) {
			d.Set(grantPrivilegesAttr, privilegesSet)
			break
		}
	}
	log.Printf("[DEBUG] Collected table grants")

	return nil
}

func readCallableGrants(db *DBConnection, d *schema.ResourceData) error {
	log.Printf("[DEBUG] Reading callable grants")

	var entityName, query string

	_, isUser := d.GetOk(grantUserAttr)
	schemaName := d.Get(grantSchemaAttr).(string)
	objectType := d.Get(grantObjectTypeAttr).(string)

	if isUser {
		entityName = d.Get(grantUserAttr).(string)
		query = `
	SELECT
		proname,
		decode(nvl(charindex('X',split_part(split_part(regexp_replace(replace(array_to_string(pr.proacl, '|'), '"', ''),'group '||u.usename,'__avoidGroupPrivs__'), u.usename||'=', 2) ,'/',1)), 0), 0,0,1) as execute
	FROM pg_proc_info pr
		JOIN pg_namespace nsp ON nsp.oid = pr.pronamespace,
	pg_user u
	WHERE
		nsp.nspname=$1 
		AND u.usename=$2
		AND pr.prokind=ANY($3)
`
	} else {
		entityName = d.Get(grantGroupAttr).(string)
		query = `
	SELECT
		proname,
		decode(nvl(charindex('X',split_part(split_part(replace(array_to_string(pr.proacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)), 0), 0,0,1) as execute
	FROM pg_proc_info pr
		JOIN pg_namespace nsp ON nsp.oid = pr.pronamespace,
	pg_group gr
	WHERE
		nsp.nspname=$1 
    AND gr.groname=$2
		AND pr.prokind=ANY($3)
`
	}

	callables := stripArgumentsFromCallablesDefinitions(d.Get(grantObjectsAttr).(*schema.Set))

	rows, err := db.Query(query, schemaName, entityName, pq.Array(grantObjectTypesCodes[objectType]))
	if err != nil {
		return err
	}

	contains := func(callables []string, objName string) bool {
		for _, callable := range callables {
			if callable == objName {
				return true
			}
		}
		return false
	}
	defer rows.Close()

	privilegesSet := schema.NewSet(schema.HashString, nil)
	for rows.Next() {
		var objName string
		var callableExecute bool

		if err := rows.Scan(&objName, &callableExecute); err != nil {
			return err
		}
		if len(callables) > 0 && !contains(callables, objName) {
			continue
		}

		if callableExecute {
			privilegesSet.Add("execute")
		}
	}

	if !privilegesSet.Equal(d.Get(grantPrivilegesAttr).(*schema.Set)) {
		d.Set(grantPrivilegesAttr, privilegesSet)
	}
	log.Printf("[DEBUG] Reading callable grants - Done")

	return nil
}

func readLanguageGrants(db *DBConnection, d *schema.ResourceData) error {
	log.Printf("[DEBUG] Reading language grants")

	var entityName, query string

	_, isUser := d.GetOk(grantUserAttr)

	if isUser {
		entityName = d.Get(grantUserAttr).(string)
		query = `
  SELECT
		lanname,
    decode(nvl(charindex('U',split_part(split_part(regexp_replace(replace(array_to_string(lg.lanacl, '|'), '"', ''),'group '||u.usename,'__avoidGroupPrivs__'), u.usename||'=', 2) ,'/',1)), 0), 0,0,1) as usage
  FROM pg_language lg, pg_user u
  WHERE
    u.usename=$1
`
	} else {
		entityName = d.Get(grantGroupAttr).(string)
		query = `
  SELECT
		lanname,
    decode(nvl(charindex('U',split_part(split_part(replace(array_to_string(lg.lanacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)), 0), 0,0,1) as usage
  FROM pg_language lg, pg_group gr
  WHERE
    gr.groname=$1
`
	}

	rows, err := db.Query(query, entityName)
	if err != nil {
		return err
	}

	objects := d.Get(grantObjectsAttr).(*schema.Set)
	defer rows.Close()

	for rows.Next() {
		var objName string
		var languageUsage bool

		if err := rows.Scan(&objName, &languageUsage); err != nil {
			return err
		}

		if objects.Len() > 0 && !objects.Contains(objName) {
			continue
		}

		privilegesSet := schema.NewSet(schema.HashString, nil)
		if languageUsage {
			privilegesSet.Add("usage")
		}

		if !privilegesSet.Equal(d.Get(grantPrivilegesAttr).(*schema.Set)) {
			d.Set(grantPrivilegesAttr, privilegesSet)
			break
		}
	}
	log.Printf("[DEBUG] Reading language grants - Done")

	return nil
}

func revokeGrants(tx *sql.Tx, databaseName string, d *schema.ResourceData) error {
	query := createGrantsRevokeQuery(d, databaseName)
	_, err := tx.Exec(query)
	return err
}

func createGrants(tx *sql.Tx, databaseName string, d *schema.ResourceData) error {
	if d.Get(grantPrivilegesAttr).(*schema.Set).Len() == 0 {
		log.Printf("[DEBUG] no privileges to grant for %s", d.Get(grantGroupAttr).(string))
		return nil
	}

	query := createGrantsQuery(d, databaseName)
	_, err := tx.Exec(query)
	return err
}

func createGrantsRevokeQuery(d *schema.ResourceData, databaseName string) string {
	var query, toWhomIndicator, entityName string

	if groupName, isGroup := d.GetOk(grantGroupAttr); isGroup {
		toWhomIndicator = "GROUP"
		entityName = groupName.(string)
	} else if userName, isUser := d.GetOk(grantUserAttr); isUser {
		entityName = userName.(string)
	}

	switch strings.ToUpper(d.Get(grantObjectTypeAttr).(string)) {
	case "DATABASE":
		query = fmt.Sprintf(
			"REVOKE ALL PRIVILEGES ON DATABASE %s FROM %s %s",
			pq.QuoteIdentifier(databaseName),
			toWhomIndicator,
			pq.QuoteIdentifier(entityName),
		)
	case "DATASHARE_DATABASE":
		query = fmt.Sprintf(
			"REVOKE USAGE ON DATABASE %s FROM %s %s",
			pq.QuoteIdentifier(databaseName),
			toWhomIndicator,
			pq.QuoteIdentifier(entityName),
		)
	case "SCHEMA":
		query = fmt.Sprintf(
			"REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s %s",
			pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
			toWhomIndicator,
			pq.QuoteIdentifier(entityName),
		)
	case "TABLE":
		objects := d.Get(grantObjectsAttr).(*schema.Set)
		if objects.Len() > 0 {
			query = fmt.Sprintf(
				"REVOKE ALL PRIVILEGES ON %s %s FROM %s %s",
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				setToPgIdentList(objects, d.Get(grantSchemaAttr).(string)),
				toWhomIndicator,
				pq.QuoteIdentifier(entityName),
			)
		} else {
			query = fmt.Sprintf(
				"REVOKE ALL PRIVILEGES ON ALL %sS IN SCHEMA %s FROM %s %s",
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
				toWhomIndicator,
				pq.QuoteIdentifier(entityName),
			)
		}
	case "FUNCTION", "PROCEDURE":
		objects := d.Get(grantObjectsAttr).(*schema.Set)
		if objects.Len() > 0 {
			query = fmt.Sprintf(
				"REVOKE ALL PRIVILEGES ON %s %s FROM %s %s",
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				setToPgIdentListNotQuoted(objects, d.Get(grantSchemaAttr).(string)),
				toWhomIndicator,
				pq.QuoteIdentifier(entityName),
			)
		} else {
			query = fmt.Sprintf(
				"REVOKE ALL PRIVILEGES ON ALL %sS IN SCHEMA %s FROM %s %s",
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
				toWhomIndicator,
				pq.QuoteIdentifier(entityName),
			)
		}
	case "LANGUAGE":
		objects := d.Get(grantObjectsAttr).(*schema.Set)
		query = fmt.Sprintf(
			"REVOKE USAGE ON LANGUAGE %s FROM %s %s",
			setToPgIdentList(objects, ""),
			toWhomIndicator,
			pq.QuoteIdentifier(entityName),
		)
	}
	log.Printf("[DEBUG] Created REVOKE query: %s", query)
	return query
}

func createGrantsQuery(d *schema.ResourceData, databaseName string) string {
	var query, toWhomIndicator, entityName string
	privileges := []string{}
	for _, p := range d.Get(grantPrivilegesAttr).(*schema.Set).List() {
		privileges = append(privileges, p.(string))
	}

	if groupName, isGroup := d.GetOk(grantGroupAttr); isGroup {
		toWhomIndicator = "GROUP"
		entityName = groupName.(string)
	} else if userName, isUser := d.GetOk(grantUserAttr); isUser {
		entityName = userName.(string)
	}

	switch strings.ToUpper(d.Get(grantObjectTypeAttr).(string)) {
	case "DATABASE", "DATASHARE_DATABASE":
		query = fmt.Sprintf(
			"GRANT %s ON DATABASE %s TO %s %s",
			strings.Join(privileges, ","),
			pq.QuoteIdentifier(databaseName),
			toWhomIndicator,
			pq.QuoteIdentifier(entityName),
		)
	case "SCHEMA":
		query = fmt.Sprintf(
			"GRANT %s ON SCHEMA %s TO %s %s",
			strings.Join(privileges, ","),
			pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
			toWhomIndicator,
			pq.QuoteIdentifier(entityName),
		)
	case "TABLE", "LANGUAGE":
		objects := d.Get(grantObjectsAttr).(*schema.Set)
		if objects.Len() > 0 {
			query = fmt.Sprintf(
				"GRANT %s ON %s %s TO %s %s",
				strings.Join(privileges, ","),
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				setToPgIdentList(objects, d.Get(grantSchemaAttr).(string)),
				toWhomIndicator,
				pq.QuoteIdentifier(entityName),
			)
		} else {
			query = fmt.Sprintf(
				"GRANT %s ON ALL %sS IN SCHEMA %s TO %s %s",
				strings.Join(privileges, ","),
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
				toWhomIndicator,
				pq.QuoteIdentifier(entityName),
			)
		}
	case "FUNCTION", "PROCEDURE":
		objects := d.Get(grantObjectsAttr).(*schema.Set)
		if objects.Len() > 0 {
			query = fmt.Sprintf(
				"GRANT %s ON %s %s TO %s %s",
				strings.Join(privileges, ","),
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				setToPgIdentListNotQuoted(objects, d.Get(grantSchemaAttr).(string)),
				toWhomIndicator,
				pq.QuoteIdentifier(entityName),
			)
		} else {
			query = fmt.Sprintf(
				"GRANT %s ON ALL %sS IN SCHEMA %s TO %s %s",
				strings.Join(privileges, ","),
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
				toWhomIndicator,
				pq.QuoteIdentifier(entityName),
			)
		}
	}

	log.Printf("[DEBUG] Created GRANT query: %s", query)
	return query
}

func generateGrantID(d *schema.ResourceData) string {
	parts := []string{}

	if _, isGroup := d.GetOk(grantGroupAttr); isGroup {
		parts = append(parts, fmt.Sprintf("gn:%s", d.Get(grantGroupAttr).(string)))
	}

	if _, isUser := d.GetOk(grantUserAttr); isUser {
		parts = append(parts, fmt.Sprintf("un:%s", d.Get(grantUserAttr).(string)))
	}

	objectType := fmt.Sprintf("ot:%s", d.Get(grantObjectTypeAttr).(string))
	parts = append(parts, objectType)

	if objectType != "ot:database" && objectType != "ot:language" && objectType != "ot:datashare_database" {
		parts = append(parts, d.Get(grantSchemaAttr).(string))
	}

	for _, object := range d.Get(grantObjectsAttr).(*schema.Set).List() {
		parts = append(parts, object.(string))
	}

	return strings.Join(parts, "_")
}
