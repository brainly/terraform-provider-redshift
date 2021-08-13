package redshift

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

func redshiftDatashare() *schema.Resource {
	return &schema.Resource{
		Description: `
Defines a Redshift datashare. Datashares allows a Redshift cluster (the "consumer") to
read data stored in another Redshift cluster (the "producer"). For more information, see
https://docs.aws.amazon.com/redshift/latest/dg/datashare-overview.html

The redshift_datashare resource should be defined on the producer cluster.

Note: Data sharing is only supported on certain Redshift instance families,
such as RA3.
`,
		Exists: RedshiftResourceExistsFunc(resourceRedshiftDatashareExists),
		Create: RedshiftResourceFunc(resourceRedshiftDatashareCreate),
		Read:   RedshiftResourceFunc(resourceRedshiftDatashareRead),
		Update: RedshiftResourceFunc(resourceRedshiftDatashareUpdate),
		Delete: RedshiftResourceFunc(resourceRedshiftDatashareDelete),
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Description: "The name of the datashare.",
				Required:    true,
				ForceNew:    true,
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			"owner": {
				Type:        schema.TypeString,
				Description: "The user who owns the datashare.",
				Optional:    true,
				Computed:    true,
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			"publicly_accessible": {
				Type:        schema.TypeBool,
				Description: "Specifies whether the datashare can be shared to clusters that are publicly accessible. Default is `false`.",
				Optional:    true,
				Default:     false,
			},
			"producer_account": {
				Type:        schema.TypeString,
				Description: "The ID for the datashare producer account.",
				Computed:    true,
			},
			"producer_namespace": {
				Type:        schema.TypeString,
				Description: "The unique cluster identifier for the datashare producer cluster.",
				Computed:    true,
			},
			"created": {
				Type:        schema.TypeString,
				Description: "The date when datashare was created",
				Computed:    true,
			},
			"schema": {
				Type:        schema.TypeSet,
				Optional:    true,
				Computed:    true,
				Description: "Defines which objects in the specified schema are exposed to the data share",
				Set:         resourceRedshiftDatashareSchemaHash,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:        schema.TypeString,
							Required:    true,
							Description: "The name of the schema",
							StateFunc: func(val interface{}) string {
								return strings.ToLower(val.(string))
							},
						},
						"mode": {
							Type:     schema.TypeString,
							Required: true,
							Description: "Configures how schema objects will be exposed to the datashare. Must be either `auto` or `manual`.\n\n" +
								"  In `auto` mode, all tables, views, and UDFs will be exposed to the datashare, and Redshift will automatically expose new tables, views, and functions in the schema to the datashare (without requiring `terraform apply` to be run again).\n\n" +
								"  In `manual` mode, only the `tables` and `functions` explicitly declared in the `schema` block will be exposed to the datashare.",
							StateFunc: func(val interface{}) string {
								return strings.ToLower(val.(string))
							},
							ValidateFunc: validation.StringInSlice([]string{
								"auto",
								"manual",
							}, false),
						},
						"tables": {
							Type:        schema.TypeSet,
							Description: "Tables and views that are exposed to the datashare. You should configure this attribute explicitly when using `manual` mode. When using `auto` mode, this is treated as a computed attribute and you should not explicitly declare it.",
							Optional:    true,
							Computed:    true,
							Set:         schema.HashString,
							Elem:        &schema.Schema{Type: schema.TypeString},
						},
						"functions": {
							Type:        schema.TypeSet,
							Description: "UDFs that are to exposed to the datashare. You should configure this attribute explicitly when using `manual` mode. When using `auto` mode, this is treated as a computed attribute and you should not explicitly declare it.",
							Optional:    true,
							Computed:    true,
							Set:         schema.HashString,
							Elem:        &schema.Schema{Type: schema.TypeString},
						},
					},
				},
			},
		},
	}
}

func resourceRedshiftDatashareSchemaHash(v interface{}) int {
	var buf bytes.Buffer
	m := v.(map[string]interface{})
	buf.WriteString(fmt.Sprintf("%s-", m["name"].(string)))
	buf.WriteString(fmt.Sprintf("%s-", m["mode"].(string)))

	// Sort the tables/functions sets to make the hash more deterministic
	if v, ok := m["tables"]; ok {
		vs := v.(*schema.Set).List()
		s := make([]string, len(vs))
		for i, raw := range vs {
			s[i] = raw.(string)
		}
		sort.Strings(s)

		for _, v := range s {
			buf.WriteString(fmt.Sprintf("%s-", v))
		}
	}

	if v, ok := m["functions"]; ok {
		vs := v.(*schema.Set).List()
		s := make([]string, len(vs))
		for i, raw := range vs {
			s[i] = raw.(string)
		}
		sort.Strings(s)

		for _, v := range s {
			buf.WriteString(fmt.Sprintf("%s-", v))
		}
	}
	return schema.HashString(buf.String())
}

func resourceRedshiftDatashareExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	var name string
	query := "SELECT share_name FROM svv_datashares WHERE share_type='OUTBOUND' AND share_id=$1"
	log.Printf("[DEBUG] check if datashare exists: %s\n", query)
	err := db.QueryRow(query, d.Id()).Scan(&name)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func resourceRedshiftDatashareCreate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	shareName := d.Get("name").(string)

	log.Println("[DEBUG] Creating datashare")
	query := fmt.Sprintf("CREATE DATASHARE %s SET PUBLICACCESSIBLE = %t", pq.QuoteIdentifier(shareName), d.Get("publicly_accessible").(bool))

	if _, err := tx.Exec(query); err != nil {
		return err
	}

	var shareId string
	query = "SELECT share_id FROM SVV_DATASHARES WHERE share_type = 'OUTBOUND' AND share_name = $1"
	log.Println("[DEBUG] getting datashare id")
	if err := tx.QueryRow(query, strings.ToLower(shareName)).Scan(&shareId); err != nil {
		return err
	}

	d.SetId(shareId)

	if owner, ownerIsSet := d.GetOk("owner"); ownerIsSet {
		log.Println("[DEBUG] Setting datashare owner")
		_, err = tx.Exec(fmt.Sprintf("ALTER DATASHARE %s OWNER TO %s", pq.QuoteIdentifier(strings.ToLower(shareName)), pq.QuoteIdentifier(strings.ToLower(owner.(string)))))
		if err != nil {
			return err
		}
	}

	for _, schema := range d.Get("schema").(*schema.Set).List() {
		err = addSchemaToDatashare(tx, shareName, schema.(map[string]interface{}))
		if err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftDatashareRead(db, d)
}

func addSchemaToDatashare(tx *sql.Tx, shareName string, m map[string]interface{}) error {
	err := resourceRedshiftDatashareAddSchema(tx, shareName, m)
	if err != nil {
		return err
	}
	err = resourceRedshiftDatashareAddTables(tx, shareName, m)
	if err != nil {
		return err
	}
	err = resourceRedshiftDatashareAddFunctions(tx, shareName, m)
	return err
}

func resourceRedshiftDatashareAddSchema(tx *sql.Tx, shareName string, m map[string]interface{}) error {
	schemaName := m["name"].(string)
	mode := m["mode"].(string)
	query := fmt.Sprintf("ALTER DATASHARE %s ADD SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	if err != nil {
		return err
	}
	if mode == "auto" {
		query = fmt.Sprintf("ALTER DATASHARE %s SET INCLUDENEW = TRUE FOR SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
		log.Printf("[DEBUG] %s\n", query)
		_, err = tx.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func resourceRedshiftDatashareAddTables(tx *sql.Tx, shareName string, m map[string]interface{}) error {
	schemaName := m["name"].(string)
	mode := m["mode"].(string)
	switch mode {
	case "auto":
		return resourceRedshiftDatashareAddAllTables(tx, shareName, schemaName)
	case "manual":
		log.Println("[DEBUG] Adding individual tables to datashare")
		for _, tableName := range m["tables"].(*schema.Set).List() {
			err := resourceRedshiftDatashareAddTable(tx, shareName, schemaName, tableName.(string))
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("Unsupported datashare schema mode: %s", mode)
	}
	return nil
}

func resourceRedshiftDatashareAddFunctions(tx *sql.Tx, shareName string, m map[string]interface{}) error {
	schemaName := m["name"].(string)
	mode := m["mode"].(string)
	switch mode {
	case "auto":
		return resourceRedshiftDatashareAddAllFunctions(tx, shareName, schemaName)
	case "manual":
		log.Println("[DEBUG] Adding individual functions to datashare")
		for _, functionName := range m["functions"].(*schema.Set).List() {
			err := resourceRedshiftDatashareAddFunction(tx, shareName, schemaName, functionName.(string))
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("Unsupported datashare schema mode: %s", mode)
	}
	return nil
}

func resourceRedshiftDatashareAddAllFunctions(tx *sql.Tx, shareName string, schemaName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s ADD ALL FUNCTIONS IN SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareAddFunction(tx *sql.Tx, shareName string, schemaName string, functionName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s ADD FUNCTION %s.%s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(functionName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareAddAllTables(tx *sql.Tx, shareName string, schemaName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s ADD ALL TABLES IN SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareAddTable(tx *sql.Tx, shareName string, schemaName string, tableName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s ADD TABLE %s.%s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(tableName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func removeSchemaFromDatashare(tx *sql.Tx, shareName string, m map[string]interface{}) error {
	err := resourceRedshiftDatashareRemoveAllFunctions(tx, shareName, m)
	if err != nil {
		return err
	}
	err = resourceRedshiftDatashareRemoveAllTables(tx, shareName, m)
	if err != nil {
		return err
	}
	err = resourceRedshiftDatashareRemoveSchema(tx, shareName, m)
	return err
}

func resourceRedshiftDatashareRemoveAllFunctions(tx *sql.Tx, shareName string, m map[string]interface{}) error {
	schemaName := m["name"].(string)
	query := fmt.Sprintf("ALTER DATASHARE %s REMOVE ALL FUNCTIONS IN SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareRemoveFunction(tx *sql.Tx, shareName string, schemaName string, functionName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s REMOVE FUNCTION %s.%s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(functionName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareRemoveAllTables(tx *sql.Tx, shareName string, m map[string]interface{}) error {
	schemaName := m["name"].(string)
	query := fmt.Sprintf("ALTER DATASHARE %s REMOVE ALL TABLES IN SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareRemoveTable(tx *sql.Tx, shareName string, schemaName string, tableName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s REMOVE TABLE %s.%s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(tableName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareRemoveSchema(tx *sql.Tx, shareName string, m map[string]interface{}) error {
	schemaName := m["name"].(string)
	query := fmt.Sprintf("ALTER DATASHARE %s REMOVE SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareRead(db *DBConnection, d *schema.ResourceData) error {
	var shareName, owner, producerAccount, producerNamespace, created string
	var publicAccessible bool

	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	log.Println("[DEBUG] reading datashare")
	err = tx.QueryRow(`
		SELECT
		  trim(svv_datashares.share_name),
			trim(pg_user.usename),
			svv_datashares.is_publicaccessible,
			TRIM(COALESCE(svv_datashares.producer_account, '')),
			TRIM(COALESCE(svv_datashares.producer_namespace, '')),
			REPLACE(TO_CHAR(svv_datashares.createdate, 'YYYY-MM-DD HH24:MI:SS'), ' ', 'T') || 'Z'
		FROM svv_datashares
		LEFT JOIN pg_user ON svv_datashares.share_owner = pg_user.usesysid
		WHERE share_type = 'OUTBOUND'
		AND share_id = $1`, d.Id()).Scan(&shareName, &owner, &publicAccessible, &producerAccount, &producerNamespace, &created)
	if err != nil {
		return err
	}

	d.Set("name", shareName)
	d.Set("owner", owner)
	d.Set("publicly_accessible", publicAccessible)
	d.Set("producer_account", producerAccount)
	d.Set("producer_namespace", producerNamespace)
	d.Set("created", created)

	// TODO read schemas
	if err = readDatashareSchemas(tx, shareName, d); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func readDatashareSchemas(tx *sql.Tx, shareName string, d *schema.ResourceData) error {
	// Run a single query to fetch all datashare object info.
	// Order doesn't matter here since
	//   a) we're storing the data in sets, and
	//   b) Redshift won't allow you to add schema objects to a datashare until after you've added the schema itself,
	//      so if we see a table/view/function/etc we can safely assume the schema is also added.
	rows, err := tx.Query(`
		SELECT
		  object_name,
			object_type,
			COALESCE(include_new, FALSE)
		FROM svv_datashare_objects
		WHERE share_type = 'OUTBOUND'
		AND share_name = $1
	`, shareName)
	if err != nil {
		return err
	}
	defer rows.Close()
	schemasByName := make(map[string]map[string]interface{})
	for rows.Next() {
		var objectName, objectType string
		var includeNew bool
		if err = rows.Scan(&objectName, &objectType, &includeNew); err != nil {
			return err
		}

		// resolve schema name
		objectNameSlice, err := splitCsvAndTrim(objectName, '.')
		if err != nil {
			return fmt.Errorf("Unable to parse datashare object name, %w", err)
		}
		if len(objectNameSlice) < 1 || len(objectNameSlice) > 2 {
			return fmt.Errorf("Unable to parse datashare object name")
		}
		schemaName := objectNameSlice[0]
		objectName = objectNameSlice[len(objectNameSlice)-1]

		// get/create schema entry
		schemaDef, ok := schemasByName[schemaName]
		if !ok {
			// schema entry doesn't exist so create it
			schemaDef = make(map[string]interface{})
			schemaDef["name"] = schemaName
			schemaDef["tables"] = schema.NewSet(schema.HashString, make([]interface{}, 0))
			schemaDef["functions"] = schema.NewSet(schema.HashString, make([]interface{}, 0))
			schemasByName[schemaName] = schemaDef
		}

		// now finally we can populate the schema info
		switch strings.ToLower(objectType) {
		case "schema":
			if includeNew {
				schemaDef["mode"] = "auto"
			} else {
				schemaDef["mode"] = "manual"
			}
		case "table", "view", "late binding view", "materialized view":
			schemaDef["tables"].(*schema.Set).Add(objectName)
		case "function":
			schemaDef["functions"].(*schema.Set).Add(objectName)
		default:
			log.Printf("[WARN] Ignoring datashare object %s.%s with type %s\n", schemaName, objectName, objectType)
		}
	}

	// convert map to set
	schemas := schema.NewSet(resourceRedshiftDatashareSchemaHash, nil)
	for _, schemaDef := range schemasByName {
		schemas.Add(schemaDef)
	}
	d.Set("schema", schemas)
	return nil
}

func resourceRedshiftDatashareUpdate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := setDatashareName(tx, d); err != nil {
		return err
	}

	if err := setDatashareOwner(tx, d); err != nil {
		return err
	}

	if err := setDatasharePubliclyAccessble(tx, d); err != nil {
		return err
	}

	if err := setDatashareSchemas(tx, d); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftDatashareRead(db, d)
}

func setDatashareName(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange("name") {
		return nil
	}
	oldRaw, newRaw := d.GetChange("name")
	oldValue := oldRaw.(string)
	newValue := newRaw.(string)
	if newValue == "" {
		return fmt.Errorf("Error setting datashare name to an empty string")
	}
	query := fmt.Sprintf("ALTER DATASHARE %s RENAME TO %s", pq.QuoteIdentifier(oldValue), pq.QuoteIdentifier(newValue))
	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("Error updating datashare NAME :%w", err)
	}
	return nil
}

func setDatashareOwner(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange("owner") {
		return nil
	}
	shareName := d.Get("name").(string)
	_, newRaw := d.GetChange("owner")
	newValue := newRaw.(string)
	if newValue == "" {
		newValue = "CURRENT_USER"
	} else {
		newValue = pq.QuoteIdentifier(newValue)
	}

	query := fmt.Sprintf("ALTER DATASHARE %s OWNER TO %s", pq.QuoteIdentifier(shareName), newValue)
	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("Error updating datashare OWNER :%w", err)
	}
	return nil
}

func setDatasharePubliclyAccessble(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange("publicly_accessible") {
		return nil
	}

	shareName := d.Get("name").(string)
	newValue := d.Get("publicly_accessible").(bool)
	query := fmt.Sprintf("ALTER DATASHARE %s SET PUBLICACCESSIBLE %t", pq.QuoteIdentifier(shareName), newValue)
	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("Error updating datashare PUBLICACCESSBILE :%w", err)
	}
	return nil
}

func setDatashareSchemas(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange("schema") {
		return nil
	}
	oldRaw, newRaw := d.GetChange("schema")
	if oldRaw == nil {
		oldRaw = schema.NewSet(resourceRedshiftDatashareSchemaHash, nil)
	}
	if newRaw == nil {
		newRaw = schema.NewSet(resourceRedshiftDatashareSchemaHash, nil)
	}
	oldCollapsed, err := resourceRedshiftDatashareCollapseSchemas(oldRaw.(*schema.Set))
	if err != nil {
		return err
	}
	newCollapsed, err := resourceRedshiftDatashareCollapseSchemas(newRaw.(*schema.Set))
	if err != nil {
		return err
	}

	add, remove, modify := computeDatashareSchemaChanges(oldCollapsed, newCollapsed)
	shareName := d.Get("name").(string)
	for _, s := range add.List() {
		if err := addSchemaToDatashare(tx, shareName, s.(map[string]interface{})); err != nil {
			return err
		}
	}
	for _, s := range remove.List() {
		if err := removeSchemaFromDatashare(tx, shareName, s.(map[string]interface{})); err != nil {
			return err
		}
	}

	// For modifications, we need to see what's changed
	oldCollapsedMap := setToMap(oldCollapsed, "name")
	for _, s := range modify.List() {
		after := s.(map[string]interface{})
		schemaName := after["name"].(string)
		before := oldCollapsedMap[schemaName]
		if err := updateDatashareSchemaObjects(tx, shareName, before, after); err != nil {
			return err
		}
	}
	return nil
}

func updateDatashareSchemaObjects(tx *sql.Tx, shareName string, before map[string]interface{}, after map[string]interface{}) error {
	// now we just need to deal with modifications to existing datashare schemas.
	schemaName := after["name"].(string)
	if strings.ToLower(after["mode"].(string)) == "auto" {
		log.Printf("[INFO] Changing schema %s in datashare %s from manual mode to auto mode\n", schemaName, shareName)
		// short-circuit the complicated logic below because we're adding all tables/functions.
		query := fmt.Sprintf("ALTER DATASHARE %s SET INCLUDENEW = TRUE FOR SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
		log.Printf("[DEBUG] %s\n", query)
		if _, err := tx.Exec(query); err != nil {
			return err
		}
		err := resourceRedshiftDatashareAddAllTables(tx, shareName, schemaName)
		if err != nil {
			return err
		}
		return resourceRedshiftDatashareAddAllFunctions(tx, shareName, schemaName)
	}
	// manual mode. Process individual table/view changes.
	if strings.ToLower(before["mode"].(string)) == "auto" {
		log.Printf("[INFO] Changing schema %s in datashare %s from auto mode to manual mode\n", schemaName, shareName)
		query := fmt.Sprintf("ALTER DATASHARE %s SET INCLUDENEW = FALSE FOR SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
		log.Printf("[DEBUG] %s\n", query)
		if _, err := tx.Exec(query); err != nil {
			return err
		}
	}
	oldCollapsed := schema.NewSet(resourceRedshiftDatashareSchemaHash, nil)
	oldCollapsed.Add(before)
	oldExpanded := resourceRedshiftDatashareExpandSchemas(oldCollapsed)
	newCollapsed := schema.NewSet(resourceRedshiftDatashareSchemaHash, nil)
	newCollapsed.Add(after)
	newExpanded := resourceRedshiftDatashareExpandSchemas(newCollapsed)

	remove := oldExpanded.Difference(newExpanded).List()
	for _, object := range remove {
		m := object.(map[string]interface{})
		schemaName := m["name"].(string)
		if tables, ok := m["tables"]; ok {
			for _, tableName := range tables.(*schema.Set).List() {
				err := resourceRedshiftDatashareRemoveTable(tx, shareName, schemaName, tableName.(string))
				if err != nil {
					return err
				}
			}
		}
		if functions, ok := m["functions"]; ok {
			for _, functionName := range functions.(*schema.Set).List() {
				err := resourceRedshiftDatashareRemoveFunction(tx, shareName, schemaName, functionName.(string))
				if err != nil {
					return err
				}
			}
		}
	}

	add := newExpanded.Difference(oldExpanded).List()
	for _, object := range add {
		m := object.(map[string]interface{})
		schemaName := m["name"].(string)
		if tables, ok := m["tables"]; ok {
			for _, tableName := range tables.(*schema.Set).List() {
				err := resourceRedshiftDatashareAddTable(tx, shareName, schemaName, tableName.(string))
				if err != nil {
					return err
				}
			}
		}
		if functions, ok := m["functions"]; ok {
			for _, functionName := range functions.(*schema.Set).List() {
				err := resourceRedshiftDatashareAddFunction(tx, shareName, schemaName, functionName.(string))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func computeDatashareSchemaChanges(old *schema.Set, new *schema.Set) (add *schema.Set, remove *schema.Set, modify *schema.Set) {
	add = schema.NewSet(resourceRedshiftDatashareSchemaHash, nil)
	remove = schema.NewSet(resourceRedshiftDatashareSchemaHash, nil)
	modify = schema.NewSet(resourceRedshiftDatashareSchemaHash, nil)

	oldNames := schema.NewSet(schema.HashString, nil)
	for _, s := range old.List() {
		m := s.(map[string]interface{})
		oldNames.Add(m["name"])
	}
	newNames := schema.NewSet(schema.HashString, nil)
	for _, s := range new.List() {
		m := s.(map[string]interface{})
		newNames.Add(m["name"])
	}
	removeNames := oldNames.Difference(newNames)
	addNames := newNames.Difference(oldNames)

	// populate remove result
	for _, s := range old.List() {
		m := s.(map[string]interface{})
		if removeNames.Contains(m["name"]) {
			remove.Add(s)
		}
	}

	// populate add/modify result
	for _, s := range new.List() {
		m := s.(map[string]interface{})
		if addNames.Contains(m["name"]) {
			add.Add(s)
		} else {
			modify.Add(s)
		}
	}

	return
}

func resourceRedshiftDatashareExpandSchemas(schemas *schema.Set) *schema.Set {
	keysToExpand := []string{"tables", "functions"}
	normalized := schema.NewSet(resourceRedshiftDatashareSchemaHash, nil)
	for _, rawObject := range schemas.List() {
		m := rawObject.(map[string]interface{})
		for _, key := range keysToExpand {
			item, exists := m[key]
			if exists {
				for _, v := range item.(*schema.Set).List() {
					newV := schema.NewSet(schema.HashString, nil)
					newV.Add(v)
					newSchemaConfig := resourceRedshiftDatashareCopySchemaObject(m, key, newV)
					normalized.Add(newSchemaConfig)
				}
			}
		}
	}
	return normalized
}

func resourceRedshiftDatashareCollapseSchemas(schemas *schema.Set) (*schema.Set, error) {
	keysToCollapse := []string{"tables", "functions"}
	schemasByName := make(map[string]map[string]interface{})
	for _, rawObject := range schemas.List() {
		m := rawObject.(map[string]interface{})
		name := m["name"].(string)
		current, found := schemasByName[name]
		if !found {
			schemasByName[name] = m
			current = m
		} else {
			// Due to some weirdness with how schema.TypeSet hashing works, we can end up in a situation where we have
			// multiple attribute blocks for the same datashare schema.
			// We're fine as long as all of the blocks use the same mode.
			if current["mode"] != m["mode"] {
				return nil, fmt.Errorf("Found multiple schema declarations for schema %s with different modes.", name)
			}
		}
		for _, key := range keysToCollapse {
			if currentObjects, found := current[key]; found {
				if objects, ok := m[key]; ok {
					current[key] = currentObjects.(*schema.Set).Union(objects.(*schema.Set))
				}
			} else {
				if objects, ok := m[key]; ok {
					current[key] = objects
				}
			}
		}
	}
	results := schema.NewSet(resourceRedshiftDatashareSchemaHash, nil)
	for _, m := range schemasByName {
		results.Add(m)
	}
	return results, nil
}

func resourceRedshiftDatashareCopySchemaObject(src map[string]interface{}, k string, v interface{}) map[string]interface{} {
	keysToCopy := []string{"name", "mode"}
	dst := make(map[string]interface{})
	for _, key := range keysToCopy {
		if val, ok := src[key]; ok {
			dst[key] = val
		}
	}
	if k != "" {
		dst[k] = v
	}
	return dst
}

func resourceRedshiftDatashareDelete(db *DBConnection, d *schema.ResourceData) error {
	shareName := d.Get("name").(string)
	log.Println("[DEBUG] deleting datashare")
	query := fmt.Sprintf("DROP DATASHARE %s", pq.QuoteIdentifier(shareName))
	_, err := db.Exec(query)
	return err
}
