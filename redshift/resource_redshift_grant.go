package redshift

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
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

	grantToPublicName = "public"
)

var grantAllowedObjectTypes = []string{
	"table",
	"schema",
	"database",
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
Defines access privileges for users and  groups. Privileges include access options such as being able to read data in tables and views, write data, create tables, and drop tables. Use this command to give specific privileges for a table, database, schema, function, procedure, language, or column.
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
				ValidateFunc: validation.StringDoesNotMatch(regexp.MustCompile("^(?i)public$"), "User name cannot be 'public'. To use GRANT ... TO PUBLIC set the group name to 'public' instead."),
			},
			grantGroupAttr: {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ExactlyOneOf: []string{grantUserAttr, grantGroupAttr},
				Description:  "The name of the group to grant privileges on. Either `group` or `user` parameter must be set. Settings the group name to `public` or `PUBLIC` (it is case insensitive in this case) will result in a `GRANT ... TO PUBLIC` statement.",
				StateFunc: func(val interface{}) string {
					name := val.(string)
					if strings.ToLower(name) == grantToPublicName {
						return strings.ToLower(name)
					}
					return name
				},
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
				Description: "The objects upon which to grant the privileges. An empty list (the default) means to grant permissions on all objects of the specified type. Ignored when `object_type` is one of (`database`, `schema`).",
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
	objects := d.Get(grantObjectsAttr).(*schema.Set).List()

	privileges := []string{}
	for _, p := range d.Get(grantPrivilegesAttr).(*schema.Set).List() {
		privileges = append(privileges, p.(string))
	}

	// validate parameters
	if (objectType == "table" || objectType == "function" || objectType == "procedure") && schemaName == "" {
		return fmt.Errorf("parameter `%s` is required for objects of type table, function and procedure", grantSchemaAttr)
	}

	if (objectType == "database" || objectType == "schema") && len(objects) > 0 {
		return fmt.Errorf("cannot specify `%s` when `%s` is `database` or `schema`", grantObjectsAttr, grantObjectTypeAttr)
	}

	if objectType == "language" && len(objects) == 0 {
		return fmt.Errorf("parameter `%s` is required for objects of type language", grantObjectsAttr)
	}

	if !validatePrivileges(privileges, objectType) {
		return fmt.Errorf("Invalid privileges list %v for object of type %s", privileges, objectType)
	}

	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := revokeGrants(tx, db.client.databaseName, d); err != nil {
		return err
	}

	if err := createGrants(tx, db.client.databaseName, d); err != nil {
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

	if err := revokeGrants(tx, db.client.databaseName, d); err != nil {
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
	var databaseCreate, databaseTemp bool

	_, isUser := d.GetOk(grantUserAttr)

	if isUser {
		entityName = d.Get(grantUserAttr).(string)
		query = `
  SELECT
    decode(charindex('C',split_part(split_part(regexp_replace(replace(array_to_string(db.datacl, '|'), '"', ''),'group '||u.usename,'__avoidGroupPrivs__'), u.usename||'=', 2) ,'/',1)), 0,0,1) as create,
    decode(charindex('T',split_part(split_part(regexp_replace(replace(array_to_string(db.datacl, '|'), '"', ''),'group '||u.usename,'__avoidGroupPrivs__'), u.usename||'=', 2) ,'/',1)), 0,0,1) as temporary
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
    decode(charindex('T',split_part(split_part(replace(array_to_string(db.datacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)), 0,0,1) as temporary
  FROM pg_database db, pg_group gr
  WHERE
    db.datname=$1 
    AND gr.groname=$2
`
	}

	queryArgs := []interface{}{db.client.databaseName, entityName}

	// Handle GRANT TO PUBLIC
	if isGrantToPublic(d) {
		query = `
  SELECT
    decode(charindex('C',split_part(split_part(regexp_replace(replace(array_to_string(db.datacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)), 0,0,1) as create,
    decode(charindex('T',split_part(split_part(regexp_replace(replace(array_to_string(db.datacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)), 0,0,1) as temporary
  FROM pg_database db
  WHERE
    db.datname=$1 
`
		queryArgs = []interface{}{db.client.databaseName}
	}

	if err := db.QueryRow(query, queryArgs...).Scan(&databaseCreate, &databaseTemp); err != nil {
		return err
	}

	privileges := []string{}
	appendIfTrue(databaseCreate, "create", &privileges)
	appendIfTrue(databaseTemp, "temporary", &privileges)

	log.Printf("[DEBUG] Collected database '%s' privileges for %s: %v", db.client.databaseName, entityName, privileges)

	d.Set(grantPrivilegesAttr, privileges)

	return nil
}

func readSchemaGrants(db *DBConnection, d *schema.ResourceData) error {
	var entityName, query string
	var schemaCreate, schemaUsage, schemaAlter bool

	_, isUser := d.GetOk(grantUserAttr)
	schemaName := d.Get(grantSchemaAttr).(string)

	if isUser {
		entityName = d.Get(grantUserAttr).(string)
		query = `
	SELECT
		decode(charindex('C',split_part(split_part(regexp_replace(replace(array_to_string(ns.nspacl, '|'), '"', ''),'group '||u.usename,'__avoidGroupPrivs__'), u.usename||'=', 2) ,'/',1)), 0,0,1) as create,
		decode(charindex('U',split_part(split_part(regexp_replace(replace(array_to_string(ns.nspacl, '|'), '"', ''),'group '||u.usename,'__avoidGroupPrivs__'), u.usename||'=', 2) ,'/',1)), 0,0,1) as usage,
		decode(charindex('A',split_part(split_part(regexp_replace(replace(array_to_string(ns.nspacl, '|'), '"', ''),'group '||u.usename,'__avoidGroupPrivs__'), u.usename||'=', 2) ,'/',1)), 0,0,1) as alter
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
    decode(charindex('U',split_part(split_part(replace(array_to_string(ns.nspacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), 0,0,1) as usage,
    decode(charindex('A',split_part(split_part(replace(array_to_string(ns.nspacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), 0,0,1) as alter
  FROM pg_namespace ns, pg_group gr
  WHERE
    ns.nspname=$1 
    AND gr.groname=$2
`
	}

	queryArgs := []interface{}{schemaName, entityName}

	// Handle GRANT TO PUBLIC
	if isGrantToPublic(d) {
		query = `
			SELECT
				decode(charindex('C',split_part(split_part(regexp_replace(replace(array_to_string(ns.nspacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)), 0,0,1) as create,
				decode(charindex('U',split_part(split_part(regexp_replace(replace(array_to_string(ns.nspacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)), 0,0,1) as usage,
				decode(charindex('A',split_part(split_part(regexp_replace(replace(array_to_string(ns.nspacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)), 0,0,1) as alter
			FROM pg_namespace ns
			WHERE
				ns.nspname=$1
			`
		queryArgs = []interface{}{schemaName}
	}

	if err := db.QueryRow(query, queryArgs...).Scan(&schemaCreate, &schemaUsage, &schemaAlter); err != nil {
		return err
	}

	privileges := []string{}
	appendIfTrue(schemaCreate, "create", &privileges)
	appendIfTrue(schemaUsage, "usage", &privileges)
	appendIfTrue(schemaAlter, "alter", &privileges)

	log.Printf("[DEBUG] Collected schema '%s' privileges for %s: %v", schemaName, entityName, privileges)

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
	decode(charindex('A',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as alter,
    decode(charindex('r',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as select,
    decode(charindex('w',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as update,
    decode(charindex('a',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as insert,
    decode(charindex('d',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as delete,
    decode(charindex('D',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as drop,
    decode(charindex('P',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),null,0,0,0,1) as truncate,
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
    decode(charindex('A',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as alter,
    decode(charindex('r',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as select,
    decode(charindex('w',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as update,
    decode(charindex('a',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as insert,
    decode(charindex('d',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as delete,
    decode(charindex('D',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as drop,
    decode(charindex('P',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), null,0, 0,0, 1) as truncate,
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
	queryArgs := []interface{}{
		pq.Array(grantObjectTypesCodes["table"]), entityName, schemaName,
	}

	if isGrantToPublic(d) {
		query = `
		SELECT
		  relname,
		  decode(charindex('A',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),null,0,0,0,1) as alter,
		  decode(charindex('r',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),null,0,0,0,1) as select,
		  decode(charindex('w',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),null,0,0,0,1) as update,
		  decode(charindex('a',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),null,0,0,0,1) as insert,
		  decode(charindex('d',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),null,0,0,0,1) as delete,
		  decode(charindex('D',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),null,0,0,0,1) as drop,
		  decode(charindex('P',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),null,0,0,0,1) as truncate,
		  decode(charindex('x',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),null,0,0,0,1) as references,
		  decode(charindex('R',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),null,0,0,0,1) as rule,
		  decode(charindex('t',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),null,0,0,0,1) as trigger

		FROM pg_class cl
		JOIN pg_namespace nsp ON nsp.oid = cl.relnamespace
		WHERE
		  cl.relkind = ANY($1)
		  AND nsp.nspname=$2
	  `
		queryArgs = []interface{}{
			pq.Array(grantObjectTypesCodes["table"]), schemaName,
		}
	}

	rows, err := db.Query(query, queryArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var objName string
		var tableAlter, tableSelect, tableUpdate, tableInsert, tableDelete, tableDrop, tableTruncate, tableReferences, tableRule, tableTrigger bool

		if err := rows.Scan(&objName, &tableAlter, &tableSelect, &tableUpdate, &tableInsert, &tableDelete, &tableDrop, &tableTruncate, &tableReferences, &tableRule, &tableTrigger); err != nil {
			return err
		}

		if objects.Len() > 0 && !objects.Contains(objName) {
			continue
		}

		privilegesSet := schema.NewSet(schema.HashString, nil)
		if tableAlter {
			privilegesSet.Add("alter")
		}
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
		if tableTruncate {
			privilegesSet.Add("truncate")
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

		log.Printf("[DEBUG] Collected table grants; table: '%v'; privileges: %v; for: %s", objName, privilegesSet.List(), entityName)
	}

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
	queryArgs := []interface{}{
		schemaName, entityName, pq.Array(grantObjectTypesCodes[objectType]),
	}

	if isGrantToPublic(d) {
		query = `
	SELECT
		proname,
		decode(nvl(charindex('X',split_part(split_part(regexp_replace(replace(array_to_string(pr.proacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)), 0), 0,0,1) as execute
	FROM pg_proc_info pr
		JOIN pg_namespace nsp ON nsp.oid = pr.pronamespace
	WHERE
		nsp.nspname=$1 
		AND pr.prokind=ANY($2)
`
		queryArgs = []interface{}{
			schemaName, pq.Array(grantObjectTypesCodes[objectType]),
		}
	}

	rows, err := db.Query(query, queryArgs...)
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

	queryArgs := []interface{}{entityName}

	// Handle GRANT TO PUBLIC
	if isGrantToPublic(d) {
		query = `
		SELECT
			  lanname,
		  decode(nvl(charindex('U',split_part(split_part(regexp_replace(replace(array_to_string(lg.lanacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)), 0), 0,0,1) as usage
		FROM pg_language lg
	  `
		queryArgs = []interface{}{}
	}

	rows, err := db.Query(query, queryArgs...)
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

	fromEntityName := pq.QuoteIdentifier(entityName)
	if isGrantToPublic(d) {
		toWhomIndicator = ""
		fromEntityName = "PUBLIC"
	}

	switch strings.ToUpper(d.Get(grantObjectTypeAttr).(string)) {
	case "DATABASE":
		query = fmt.Sprintf(
			"REVOKE ALL PRIVILEGES ON DATABASE %s FROM %s %s",
			pq.QuoteIdentifier(databaseName),
			toWhomIndicator,
			fromEntityName,
		)
	case "SCHEMA":
		query = fmt.Sprintf(
			"REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s %s",
			pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
			toWhomIndicator,
			fromEntityName,
		)
	case "TABLE":
		objects := d.Get(grantObjectsAttr).(*schema.Set)
		if objects.Len() > 0 {
			query = fmt.Sprintf(
				"REVOKE ALL PRIVILEGES ON %s %s FROM %s %s",
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				setToPgIdentList(objects, d.Get(grantSchemaAttr).(string)),
				toWhomIndicator,
				fromEntityName,
			)
		} else {
			query = fmt.Sprintf(
				"REVOKE ALL PRIVILEGES ON ALL %sS IN SCHEMA %s FROM %s %s",
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
				toWhomIndicator,
				fromEntityName,
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
				fromEntityName,
			)
		} else {
			query = fmt.Sprintf(
				"REVOKE ALL PRIVILEGES ON ALL %sS IN SCHEMA %s FROM %s %s",
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
				toWhomIndicator,
				fromEntityName,
			)
		}
	case "LANGUAGE":
		objects := d.Get(grantObjectsAttr).(*schema.Set)
		query = fmt.Sprintf(
			"REVOKE USAGE ON LANGUAGE %s FROM %s %s",
			setToPgIdentList(objects, ""),
			toWhomIndicator,
			fromEntityName,
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

	toEntityName := pq.QuoteIdentifier(entityName)
	if isGrantToPublic(d) {
		toWhomIndicator = ""
		toEntityName = "PUBLIC"
	}

	switch strings.ToUpper(d.Get(grantObjectTypeAttr).(string)) {
	case "DATABASE":
		query = fmt.Sprintf(
			"GRANT %s ON DATABASE %s TO %s %s",
			strings.Join(privileges, ","),
			pq.QuoteIdentifier(databaseName),
			toWhomIndicator,
			toEntityName,
		)
	case "SCHEMA":
		query = fmt.Sprintf(
			"GRANT %s ON SCHEMA %s TO %s %s",
			strings.Join(privileges, ","),
			pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
			toWhomIndicator,
			toEntityName,
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
				toEntityName,
			)
		} else {
			query = fmt.Sprintf(
				"GRANT %s ON ALL %sS IN SCHEMA %s TO %s %s",
				strings.Join(privileges, ","),
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
				toWhomIndicator,
				toEntityName,
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
				toEntityName,
			)
		} else {
			query = fmt.Sprintf(
				"GRANT %s ON ALL %sS IN SCHEMA %s TO %s %s",
				strings.Join(privileges, ","),
				strings.ToUpper(d.Get(grantObjectTypeAttr).(string)),
				pq.QuoteIdentifier(d.Get(grantSchemaAttr).(string)),
				toWhomIndicator,
				toEntityName,
			)
		}
	}

	log.Printf("[DEBUG] Created GRANT query: %s", query)
	return query
}

func isGrantToPublic(d *schema.ResourceData) bool {
	if _, isGroup := d.GetOk(grantGroupAttr); isGroup {
		entityName := d.Get(grantGroupAttr).(string)

		return strings.ToLower(entityName) == grantToPublicName
	}

	return false
}

func generateGrantID(d *schema.ResourceData) string {
	parts := []string{}

	if _, isGroup := d.GetOk(grantGroupAttr); isGroup {
		name := d.Get(grantGroupAttr).(string)
		if isGrantToPublic(d) {
			name = strings.ToLower(name)
		}

		parts = append(parts, fmt.Sprintf("gn:%s", name))
	}

	if _, isUser := d.GetOk(grantUserAttr); isUser {
		parts = append(parts, fmt.Sprintf("un:%s", d.Get(grantUserAttr).(string)))
	}

	objectType := fmt.Sprintf("ot:%s", d.Get(grantObjectTypeAttr).(string))
	parts = append(parts, objectType)

	if objectType != "ot:database" && objectType != "ot:language" {
		parts = append(parts, d.Get(grantSchemaAttr).(string))
	}

	for _, object := range d.Get(grantObjectsAttr).(*schema.Set).List() {
		parts = append(parts, object.(string))
	}

	return strings.Join(parts, "_")
}
