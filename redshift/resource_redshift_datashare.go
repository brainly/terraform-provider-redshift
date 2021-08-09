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
	schemaResource := v.(map[string]interface{})
	schemaName := schemaResource["name"].(string)
	return schema.HashString(schemaName)
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
		err = resourceRedshiftDatashareAddSchema(tx, d, schema.(map[string]interface{}))
		if err != nil {
			return err
		}
		err = resourceRedshiftDatashareAddTables(tx, d, schema.(map[string]interface{}))
		if err != nil {
			return err
		}
		err = resourceRedshiftDatashareAddFunctions(tx, d, schema.(map[string]interface{}))
		if err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftDatashareRead(db, d)
}

func resourceRedshiftDatashareAddSchema(tx *sql.Tx, d *schema.ResourceData, schema map[string]interface{}) error {
	shareName := d.Get("name").(string)
	schemaName := schema["name"].(string)
	mode := schema["mode"].(string)
	log.Println("[DEBUG] Adding schema to datashare")
	_, err := tx.Exec(fmt.Sprintf("ALTER DATASHARE %s ADD SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName)))
	if err != nil {
		return err
	}
	if mode == "auto" {
		_, err = tx.Exec(fmt.Sprintf("ALTER DATASHARE %s SET INCLUDENEW = TRUE FOR SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName)))
		if err != nil {
			return err
		}
	}
	return nil
}

func resourceRedshiftDatashareAddTables(tx *sql.Tx, d *schema.ResourceData, schemaConfig map[string]interface{}) error {
	shareName := d.Get("name").(string)
	schemaName := schemaConfig["name"].(string)
	mode := schemaConfig["mode"].(string)
	switch mode {
	case "auto":
		log.Println("[DEBUG] Adding all tables to datashare")
		_, err := tx.Exec(fmt.Sprintf("ALTER DATASHARE %s ADD ALL TABLES IN SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName)))
		if err != nil {
			return err
		}
	case "manual":
		log.Println("[DEBUG] Adding individual tables to datashare")
		for _, table := range schemaConfig["tables"].(*schema.Set).List() {
			_, err := tx.Exec(fmt.Sprintf("ALTER DATASHARE %s ADD TABLE %s.%s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(table.(string))))
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("Unsupported datashare schema mode: %s", mode)
	}
	return nil
}

func resourceRedshiftDatashareAddFunctions(tx *sql.Tx, d *schema.ResourceData, schemaConfig map[string]interface{}) error {
	shareName := d.Get("name").(string)
	schemaName := schemaConfig["name"].(string)
	mode := schemaConfig["mode"].(string)
	switch mode {
	case "auto":
		log.Println("[DEBUG] Adding all functions to datashare")
		_, err := tx.Exec(fmt.Sprintf("ALTER DATASHARE %s ADD ALL FUNCTIONS IN SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName)))
		if err != nil {
			return err
		}
	case "manual":
		log.Println("[DEBUG] Adding individual functions to datashare")
		for _, table := range schemaConfig["functions"].(*schema.Set).List() {
			_, err := tx.Exec(fmt.Sprintf("ALTER DATASHARE %s ADD FUNCTION %s.%s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(table.(string))))
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("Unsupported datashare schema mode: %s", mode)
	}
	return nil
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
	schemaSlice := make([]interface{}, 0)
	for _, schemaDef := range schemasByName {
		schemaSlice = append(schemaSlice, schemaDef)
	}
	schemas := schema.NewSet(resourceRedshiftDatashareSchemaHash, schemaSlice)
	d.Set("schema", schemas)
	return nil
}

func resourceRedshiftDatashareUpdate(db *DBConnection, d *schema.ResourceData) error {
	// TODO implement
	return nil
}

func resourceRedshiftDatashareDelete(db *DBConnection, d *schema.ResourceData) error {
	shareName := d.Get("name").(string)
	log.Println("[DEBUG] deleting datashare")
	query := fmt.Sprintf("DROP DATASHARE %s", pq.QuoteIdentifier(shareName))
	_, err := db.Exec(query)
	return err
}
