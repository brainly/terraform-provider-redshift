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
	schemaNameAttr            = "name"
	schemaOwnerAttr           = "owner"
	schemaQuotaAttr           = "quota"
	schemaCascadeOnDeleteAttr = "cascade_on_delete"
)

func redshiftSchema() *schema.Resource {
	return &schema.Resource{
		Description: `
A database contains one or more named schemas. Each schema in a database contains tables and other kinds of named objects. By default, a database has a single schema, which is named PUBLIC. You can use schemas to group database objects under a common name. Schemas are similar to file system directories, except that schemas cannot be nested.
`,
		Create: RedshiftResourceFunc(resourceRedshiftSchemaCreate),
		Read:   RedshiftResourceFunc(resourceRedshiftSchemaRead),
		Update: RedshiftResourceFunc(resourceRedshiftSchemaUpdate),
		Delete: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftSchemaDelete),
		),
		Exists: RedshiftResourceExistsFunc(resourceRedshiftSchemaExists),
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			schemaNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of the schema. The schema name can't be `PUBLIC`.",
				ValidateFunc: validation.StringNotInSlice([]string{
					"public",
				}, true),
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			schemaOwnerAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				Description: "Name of the schema owner.",
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			schemaQuotaAttr: {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      0,
				Description:  "The maximum amount of disk space that the specified schema can use. GB is the default unit of measurement.",
				ValidateFunc: validation.IntAtLeast(0),
				StateFunc: func(val interface{}) string {
					return fmt.Sprintf("%d", val.(int)*1024)
				},
			},
			schemaCascadeOnDeleteAttr: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Indicates to automatically drop all objects in the schema. The default action is TO NOT drop a schema if it contains any objects.",
			},
		},
	}
}

func resourceRedshiftSchemaExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	var name string
	err := db.QueryRow("SELECT nspname FROM pg_namespace WHERE oid = $1", d.Id()).Scan(&name)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func resourceRedshiftSchemaRead(db *DBConnection, d *schema.ResourceData) error {
	return resourceRedshiftSchemaReadImpl(db, d)
}

func resourceRedshiftSchemaReadImpl(db *DBConnection, d *schema.ResourceData) error {
	var schemaOwner, schemaName string
	var schemaQuota int

	err := db.QueryRow(`
			SELECT 
			  trim(nspname),
			  trim(usename),
			  COALESCE(quota, 0)
			FROM pg_namespace 
			  LEFT JOIN svv_schema_quota_state
			    ON svv_schema_quota_state.schema_id = pg_namespace.oid
			  LEFT JOIN pg_user_info
			    ON pg_user_info.usesysid = pg_namespace.nspowner
			WHERE pg_namespace.oid = $1`, d.Id()).Scan(&schemaName, &schemaOwner, &schemaQuota)

	if err != nil {
		return err
	}

	d.Set(schemaNameAttr, schemaName)
	d.Set(schemaOwnerAttr, schemaOwner)
	d.Set(schemaQuotaAttr, schemaQuota)

	return nil
}

func resourceRedshiftSchemaDelete(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)
	schemaName := d.Get(schemaNameAttr).(string)

	cascade_or_restrict := "RESTRICT"
	if cascade, ok := d.GetOk(schemaCascadeOnDeleteAttr); ok && cascade.(bool) {
		cascade_or_restrict = "CASCADE"
	}

	sql := fmt.Sprintf("DROP SCHEMA %s %s", pq.QuoteIdentifier(schemaName), cascade_or_restrict)
	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	return tx.Commit()
}

func resourceRedshiftSchemaCreate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	schemaName := d.Get(schemaNameAttr).(string)
	schemaQuota := d.Get(schemaQuotaAttr).(int)
	createOpts := []string{}

	if v, ok := d.GetOk(schemaOwnerAttr); ok {
		createOpts = append(createOpts, fmt.Sprintf("AUTHORIZATION %s", pq.QuoteIdentifier(v.(string))))
	}

	quotaValue := "QUOTA UNLIMITED"
	if schemaQuota > 0 {
		quotaValue = fmt.Sprintf("QUOTA %d GB", schemaQuota)
	}
	createOpts = append(createOpts, quotaValue)

	sql := fmt.Sprintf("CREATE SCHEMA %s %s", pq.QuoteIdentifier(schemaName), strings.Join(createOpts, " "))

	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	var schemaOID string
	if err := tx.QueryRow("SELECT oid FROM pg_namespace WHERE nspname = $1", strings.ToLower(schemaName)).Scan(&schemaOID); err != nil {
		return err
	}

	d.SetId(schemaOID)

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftSchemaReadImpl(db, d)
}

func resourceRedshiftSchemaUpdate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := setSchemaName(tx, d); err != nil {
		return err
	}

	if err := setSchemaOwner(tx, db, d); err != nil {
		return err
	}

	if err := setSchemaQuota(tx, d); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftSchemaReadImpl(db, d)
}

func setSchemaName(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(schemaNameAttr) {
		return nil
	}

	oldRaw, newRaw := d.GetChange(schemaNameAttr)
	oldValue := oldRaw.(string)
	newValue := newRaw.(string)

	if newValue == "" {
		return fmt.Errorf("Error setting schema name to an empty string")
	}

	sql := fmt.Sprintf("ALTER SCHEMA %s RENAME TO %s", pq.QuoteIdentifier(oldValue), pq.QuoteIdentifier(newValue))
	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("Error updating schema NAME: %w", err)
	}

	return nil
}

func setSchemaOwner(tx *sql.Tx, db *DBConnection, d *schema.ResourceData) error {
	if !d.HasChange(schemaOwnerAttr) {
		return nil
	}

	schemaName := d.Get(schemaNameAttr).(string)
	schemaOwner := d.Get(schemaOwnerAttr).(string)

	_, err := tx.Exec(fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(schemaOwner)))
	return err
}

func setSchemaQuota(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(schemaQuotaAttr) {
		return nil
	}

	schemaName := d.Get(schemaNameAttr).(string)
	schemaQuota := d.Get(schemaQuotaAttr).(int)

	quotaValue := "UNLIMITED"
	if schemaQuota > 0 {
		quotaValue = fmt.Sprintf("%d GB", schemaQuota)
	}

	_, err := tx.Exec(fmt.Sprintf("ALTER SCHEMA %s QUOTA %s", pq.QuoteIdentifier(schemaName), quotaValue))
	return err
}
