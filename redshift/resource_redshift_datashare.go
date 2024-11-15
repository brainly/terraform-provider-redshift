package redshift

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/lib/pq"
)

const (
	dataShareNameAttr              = "name"
	dataShareOwnerAttr             = "owner"
	dataSharePublicAccessibleAttr  = "publicly_accessible"
	dataShareProducerAccountAttr   = "producer_account"
	dataShareProducerNamespaceAttr = "producer_namespace"
	dataShareCreatedAttr           = "created"
	dataShareSchemasAttr           = "schemas"
	dataShareTablesAttr            = "tables"
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
			dataShareNameAttr: {
				Type:        schema.TypeString,
				Description: "The name of the datashare.",
				Required:    true,
				ForceNew:    true,
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			dataShareOwnerAttr: {
				Type:        schema.TypeString,
				Description: "The user who owns the datashare.",
				Optional:    true,
				Computed:    true,
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			dataSharePublicAccessibleAttr: {
				Type:        schema.TypeBool,
				Description: "Specifies whether the datashare can be shared to clusters that are publicly accessible. Default is `false`.",
				Optional:    true,
				Default:     false,
			},
			dataShareProducerAccountAttr: {
				Type:        schema.TypeString,
				Description: "The ID for the datashare producer account.",
				Computed:    true,
			},
			dataShareProducerNamespaceAttr: {
				Type:        schema.TypeString,
				Description: "The unique cluster identifier for the datashare producer cluster.",
				Computed:    true,
			},
			dataShareCreatedAttr: {
				Type:        schema.TypeString,
				Description: "The date when datashare was created",
				Computed:    true,
			},
			dataShareSchemasAttr: {
				Type:        schema.TypeSet,
				Optional:    true,
				Description: "Defines which schemas and tables are exposed to the data share.",
				Set:         schema.HashString,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"schema_name": {
							Type:        schema.TypeString,
							Required:    true,
							Description: "Schema name to include in the data share.",
							StateFunc: func(val interface{}) string {
								return strings.ToLower(val.(string))
							},
						},
						dataShareTablesAttr: {
							Type:        schema.TypeSet,
							Optional:    true,
							Description: "Specific tables in the schema to be added to the data share. If empty, all tables in the schema will be included.",
							Set:         schema.HashString,
							Elem: &schema.Schema{
								Type: schema.TypeString,
								StateFunc: func(val interface{}) string {
									return strings.ToLower(val.(string))
								},
							},
						},
					},
				},
			},
		},
	}
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

	shareName := d.Get(dataShareNameAttr).(string)

	query := fmt.Sprintf("CREATE DATASHARE %s SET PUBLICACCESSIBLE = %t", pq.QuoteIdentifier(shareName), d.Get(dataSharePublicAccessibleAttr).(bool))
	log.Printf("[DEBUG] %s\n", query)
	if _, err := tx.Exec(query); err != nil {
		return err
	}

	var shareId string
	query = "SELECT share_id FROM SVV_DATASHARES WHERE share_type = 'OUTBOUND' AND share_name = $1"
	log.Printf("[DEBUG] %s, $1=%s\n", query, strings.ToLower(shareName))
	if err := tx.QueryRow(query, strings.ToLower(shareName)).Scan(&shareId); err != nil {
		return err
	}

	d.SetId(shareId)

	if owner, ownerIsSet := d.GetOk(dataShareOwnerAttr); ownerIsSet {
		query = fmt.Sprintf("ALTER DATASHARE %s OWNER TO %s", pq.QuoteIdentifier(strings.ToLower(shareName)), pq.QuoteIdentifier(strings.ToLower(owner.(string))))
		log.Printf("[DEBUG] %s\n", query)
		_, err = tx.Exec(query)
		if err != nil {
			return err
		}
	}

	for _, schema := range d.Get(dataShareSchemasAttr).(*schema.Set).List() {
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

func addSchemaToDatashare(tx *sql.Tx, shareName string, schema map[string]interface{}) error {
    // Ensure schema_name is present and of type string
    schemaName, ok := schema["schema_name"].(string)
    if !ok || schemaName == "" {
        return fmt.Errorf("schema_name is required and must be a string")
    }

    // Add schema to datashare
    if err := resourceRedshiftDatashareAddSchema(tx, shareName, schemaName); err != nil {
        return err
    }

    // Check if specific tables are specified
    if tables, ok := schema["tables"].([]string); ok && len(tables) > 0 {
        // Add specific tables in the schema
        for _, table := range tables {
            if err := resourceRedshiftDatashareAddTable(tx, shareName, schemaName, table); err != nil {
                return err
            }
        }
    } else {
        // If no tables are specified, add all tables
        if err := resourceRedshiftDatashareAddAllTables(tx, shareName, schemaName); err != nil {
            return err
        }
    }

    // Add functions to the schema
    if err := resourceRedshiftDatashareAddAllFunctions(tx, shareName, schemaName); err != nil {
        return err
    }

    return nil
}


func resourceRedshiftDatashareAddTable(tx *sql.Tx, shareName string, schemaName string, tableName string) error {
    query := fmt.Sprintf("ALTER DATASHARE %s ADD TABLE %s.%s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(tableName))
    log.Printf("[DEBUG] %s\n", query)
    _, err := tx.Exec(query)
    return err
}

func resourceRedshiftDatashareAddSchema(tx *sql.Tx, shareName string, schemaName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s ADD SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	if err != nil {
		// if the schema is already in the datashare we get a "duplicate schema" error code. This is fine.
		if pqErr, ok := err.(*pq.Error); ok {
			if string(pqErr.Code) == pqErrorCodeDuplicateSchema {
				log.Printf("[WARN] Schema %s already exists in datashare %s\n", schemaName, shareName)
			} else {
				return err
			}
		} else {
			return err
		}
	}
	query = fmt.Sprintf("ALTER DATASHARE %s SET INCLUDENEW = TRUE FOR SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err = tx.Exec(query)
	return err
}

func resourceRedshiftDatashareAddAllFunctions(tx *sql.Tx, shareName string, schemaName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s ADD ALL FUNCTIONS IN SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareAddAllTables(tx *sql.Tx, shareName string, schemaName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s ADD ALL TABLES IN SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareRemoveTable(tx *sql.Tx, shareName string, schemaName string, tableName string) error {
    query := fmt.Sprintf("ALTER DATASHARE %s REMOVE TABLE %s.%s", 
        pq.QuoteIdentifier(shareName), 
        pq.QuoteIdentifier(schemaName), 
        pq.QuoteIdentifier(tableName))
    log.Printf("[DEBUG] %s\n", query)
    _, err := tx.Exec(query)
    return err
}

func removeSchemaFromDatashare(tx *sql.Tx, shareName string, schema map[string]interface{}) error {
    schemaName, ok := schema["schema_name"].(string)
    if !ok || schemaName == "" {
        return fmt.Errorf("schema_name is required and must be a string")
    }

    if tables, ok := schema["tables"].([]string); ok && len(tables) > 0 {
        // Remove specific tables
        for _, table := range tables {
            if err := resourceRedshiftDatashareRemoveTable(tx, shareName, schemaName, table); err != nil {
                return err
            }
        }
    } else {
        // If no specific tables were specified, remove all tables
        if err := resourceRedshiftDatashareRemoveAllTables(tx, shareName, schemaName); err != nil {
            return err
        }
    }

    if err := resourceRedshiftDatashareRemoveAllFunctions(tx, shareName, schemaName); err != nil {
        return err
    }

    return resourceRedshiftDatashareRemoveSchema(tx, shareName, schemaName)
}




func resourceRedshiftDatashareRemoveAllFunctions(tx *sql.Tx, shareName string, schemaName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s REMOVE ALL FUNCTIONS IN SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareRemoveAllTables(tx *sql.Tx, shareName string, schemaName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s REMOVE ALL TABLES IN SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareRemoveSchema(tx *sql.Tx, shareName string, schemaName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s REMOVE SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	if err != nil {
		// if the schema is not already in the datashare we get a "datashare does not contain schema" error code. This is fine.
		if pqErr, ok := err.(*pq.Error); ok {
			if string(pqErr.Code) == pqErrorCodeInvalidSchemaName {
				log.Printf("[WARN] Schema %s does not exist in datashare %s\n", schemaName, shareName)
			} else {
				return err
			}
		} else {
			return err
		}
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

	query := `
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
	AND share_id = $1`
	log.Printf("[DEBUG] %s, $1=%s\n", query, d.Id())
	err = tx.QueryRow(query, d.Id()).Scan(&shareName, &owner, &publicAccessible, &producerAccount, &producerNamespace, &created)
	if err != nil {
		return err
	}

	d.Set(dataShareNameAttr, shareName)
	d.Set(dataShareOwnerAttr, owner)
	d.Set(dataSharePublicAccessibleAttr, publicAccessible)
	d.Set(dataShareProducerAccountAttr, producerAccount)
	d.Set(dataShareProducerNamespaceAttr, producerNamespace)
	d.Set(dataShareCreatedAttr, created)

	if err = readDatashareSchemas(tx, shareName, d); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func readDatashareSchemas(tx *sql.Tx, shareName string, d *schema.ResourceData) error {
    // Query to get schemas in the datashare
    schemaQuery := `
    SELECT
        object_name
    FROM svv_datashare_objects
    WHERE share_type = 'OUTBOUND'
    AND object_type = 'schema'
    AND share_name = $1
    `
    log.Printf("[DEBUG] %s, $1=%s\n", schemaQuery, shareName)
    schemaRows, err := tx.Query(schemaQuery, shareName)
    if err != nil {
        return err
    }
    defer schemaRows.Close()

    schemas := schema.NewSet(schema.HashString, nil)
    for schemaRows.Next() {
        var schemaName string
        if err = schemaRows.Scan(&schemaName); err != nil {
            return err
        }
        // Fetch tables for this schema
        tables, err := readDatashareTables(tx, shareName, schemaName)
        if err != nil {
            return err
        }

        schemaData := map[string]interface{}{
            "schema_name": schemaName,
            "tables":      tables,
        }

        schemas.Add(schemaData)
    }

    d.Set(dataShareSchemasAttr, schemas)
    return nil
}

func readDatashareTables(tx *sql.Tx, shareName string, schemaName string) (*schema.Set, error) {
    // Query to get tables in the specific schema of the datashare
    tableQuery := `
    SELECT
        object_name
    FROM svv_datashare_objects
    WHERE share_type = 'OUTBOUND'
    AND object_type = 'table'
    AND share_name = $1
    AND schema_name = $2
    `
    log.Printf("[DEBUG] %s, $1=%s, $2=%s\n", tableQuery, shareName, schemaName)
    tableRows, err := tx.Query(tableQuery, shareName, schemaName)
    if err != nil {
        return nil, err
    }
    defer tableRows.Close()

    tables := schema.NewSet(schema.HashString, nil)
    for tableRows.Next() {
        var tableName string
        if err = tableRows.Scan(&tableName); err != nil {
            return nil, err
        }
        tables.Add(tableName)
    }

    return tables, nil
}


func resourceRedshiftDatashareUpdate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

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

func setDatashareOwner(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(dataShareOwnerAttr) {
		return nil
	}
	shareName := d.Get(dataShareNameAttr).(string)
	_, newRaw := d.GetChange(dataShareOwnerAttr)
	newValue := newRaw.(string)
	if newValue == "" {
		newValue = "CURRENT_USER"
	} else {
		newValue = pq.QuoteIdentifier(newValue)
	}

	query := fmt.Sprintf("ALTER DATASHARE %s OWNER TO %s", pq.QuoteIdentifier(shareName), newValue)
	log.Printf("[DEBUG] %s\n", query)
	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("Error updating datashare OWNER :%w", err)
	}
	return nil
}

func setDatasharePubliclyAccessble(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(dataSharePublicAccessibleAttr) {
		return nil
	}

	shareName := d.Get(dataShareNameAttr).(string)
	newValue := d.Get(dataSharePublicAccessibleAttr).(bool)
	query := fmt.Sprintf("ALTER DATASHARE %s SET PUBLICACCESSIBLE %t", pq.QuoteIdentifier(shareName), newValue)
	log.Printf("[DEBUG] %s\n", query)
	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("Error updating datashare PUBLICACCESSBILE :%w", err)
	}
	return nil
}

func setDatashareSchemas(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(dataShareSchemasAttr) {
		return nil
	}
	before, after := d.GetChange(dataShareSchemasAttr)
	if before == nil {
		before = schema.NewSet(schema.HashString, nil)
	}
	if after == nil {
		after = schema.NewSet(schema.HashString, nil)
	}

	add := after.(*schema.Set).Difference(before.(*schema.Set))
	remove := before.(*schema.Set).Difference(after.(*schema.Set))

	shareName := d.Get(dataShareNameAttr).(string)
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

	return nil
}

func resourceRedshiftDatashareDelete(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	var shareName string
	query := "SELECT share_name FROM svv_datashares WHERE share_type='OUTBOUND' AND share_id=$1"
	if err := tx.QueryRow(query, d.Id()).Scan(&shareName); err != nil {
		if err == sql.ErrNoRows {
			log.Printf("[WARN] data share with id %s does not exist.\n", d.Id())
			return nil
		}
		return err
	}
	query = fmt.Sprintf("DROP DATASHARE %s", pq.QuoteIdentifier(shareName))
	log.Printf("[DEBUG] %s\n", query)
	_, err = tx.Exec(query)
	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}
